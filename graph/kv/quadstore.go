// Copyright 2017 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/proto"
	"github.com/cayleygraph/cayley/internal/lru"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/quad/pquads"
	boom "github.com/tylertreat/BoomFilters"
)

var (
	ErrNoBucket = errors.New("kv: bucket is missing")
)

const (
	latestDataVersion = 1
	nilDataVersion    = 1
	notFound          = 0
)

type Bucket interface {
	Get(k []byte) []byte
	Put(k, v []byte) error
	ForEach(fn func(k, v []byte) error) error
}

type Tx interface {
	Bucket(name []byte) Bucket
	CreateBucket(name []byte) (Bucket, error)
	Commit() error
	Rollback() error
}

type KV interface {
	Type() string
	View() (Tx, error)
	Update() (Tx, error)
	Close() error
}

func Update(kv KV, update func(tx Tx) error) error {
	tx, err := kv.Update()
	if err != nil {
		return err
	}
	if err = update(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func View(kv KV, view func(tx Tx) error) error {
	tx, err := kv.View()
	if err != nil {
		return err
	}
	err = view(tx)
	tx.Rollback()
	return err
}

type QuadStore struct {
	db        KV
	path      string
	mapBucket map[string]map[uint64][]uint64
	valueLRU  *lru.Cache
	exists    *boom.DeletableBloomFilter
	writer    sync.Mutex

	mu            sync.RWMutex
	size          int64
	horizon       int64
	sameAsHorizon int64
	version       int64

	hashBuf  []byte
	bloomBuf []byte
	bufLock  sync.Mutex
}

func Init(kv KV, _ graph.Options) error {
	qs := &QuadStore{db: kv}
	defer qs.Close()
	if err := qs.getMetadata(); err != ErrNoBucket {
		return graph.ErrDatabaseExists
	}
	if err := qs.createBuckets(); err != nil {
		return err
	}
	if err := setVersion(qs.db, latestDataVersion); err != nil {
		return err
	}
	qs.Close()
	return nil
}

func New(kv KV, _ graph.Options) (graph.QuadStore, error) {
	qs := &QuadStore{db: kv}
	if err := qs.getMetadata(); err == ErrNoBucket {
		return nil, errors.New("kv: quadstore has not been initialised")
	} else if err != nil {
		return nil, err
	}
	if qs.version != latestDataVersion {
		return nil, errors.New("bolt: data version is out of date. Run cayleyupgrade for your config to update the data.")
	}
	qs.valueLRU = lru.New(2000)
	qs.initBloomFilter()
	qs.hashBuf = make([]byte, quad.HashSize)
	qs.bloomBuf = make([]byte, 3*8)
	return qs, nil
}

func setVersion(kv KV, version int64) error {
	return Update(kv, func(tx Tx) error {
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], uint64(version))
		b := tx.Bucket(metaBucket)

		if err := b.Put([]byte("version"), buf[:]); err != nil {
			return fmt.Errorf("couldn't write version: %v", err)
		}
		return nil
	})
}

func (qs *QuadStore) Size() int64 {
	qs.mu.RLock()
	defer qs.mu.RUnlock()
	return qs.size
}

func (qs *QuadStore) Close() error {
	err := Update(qs.db, func(tx Tx) error {
		return qs.writeHorizonAndSize(tx)
	})
	if err != nil {
		qs.db.Close()
		return err
	}
	return qs.db.Close()
}

func (qs *QuadStore) getMetadata() error {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	return View(qs.db, func(tx Tx) error {
		var err error
		qs.size, err = getInt64ForMetaKey(tx, "size", 0)
		if err != nil {
			return err
		}
		qs.version, err = getInt64ForMetaKey(tx, "version", nilDataVersion)
		if err != nil {
			return err
		}
		qs.sameAsHorizon, err = getInt64ForMetaKey(tx, "sameAsHorizon", 0)
		if err != nil {
			return err
		}
		qs.horizon, err = getInt64ForMetaKey(tx, "horizon", 0)
		return err
	})
}

func getInt64ForMetaKey(tx Tx, key string, empty int64) (int64, error) {
	return getInt64ForKey(tx, metaBucket, key, empty)
}

func getInt64ForKey(tx Tx, bucket []byte, key string, empty int64) (int64, error) {
	var out int64
	b := tx.Bucket(bucket)
	if b == nil {
		return empty, ErrNoBucket
	}
	data := b.Get([]byte(key))
	if data == nil {
		return empty, nil
	}
	buf := bytes.NewBuffer(data)
	err := binary.Read(buf, binary.LittleEndian, &out)
	if err != nil {
		return 0, err
	}
	return out, nil
}

func (qs *QuadStore) Horizon() graph.PrimaryKey {
	qs.mu.RLock()
	defer qs.mu.RUnlock()
	return graph.NewSequentialKey(qs.horizon)
}

func (qs *QuadStore) NameOf(k graph.Value) quad.Value {
	if k == nil {
		return nil
	} else if v, ok := k.(graph.PreFetchedValue); ok {
		return v.NameOf()
	}
	if v, ok := k.(Int64Value); ok {
		if v == 0 {
			if clog.V(2) {
				clog.Infof("k was 0")
			}
			return nil
		}
		var val quad.Value
		err := View(qs.db, func(tx Tx) error {
			var err error
			val, err = qs.getValFromLog(tx, uint64(v))
			return err
		})
		if err != nil {
			clog.Errorf("error getting NameOf %d: %s", v, err)
			return nil
		}
		return val
	}
	panic("unknown type of graph.Value; not meant for this quadstore. apparently a " + fmt.Sprintf("%#v", k))
}

func (qs *QuadStore) Quad(k graph.Value) quad.Quad {
	var v quad.Quad
	key, ok := k.(*proto.Primitive)
	if !ok {
		clog.Errorf("passed value was not a quad primitive")
		return quad.Quad{}
	}
	err := View(qs.db, func(tx Tx) error {
		var err error
		v, err = qs.primitiveToQuad(tx, key)
		return err
	})
	if err != nil {
		clog.Errorf("error fetching quad %#v: %s", key, err)
	}
	return v
}

func (qs *QuadStore) primitiveToQuad(tx Tx, p *proto.Primitive) (quad.Quad, error) {
	q := &quad.Quad{}
	for _, dir := range quad.Directions {
		v := p.GetDirection(dir)
		val, err := qs.getValFromLog(tx, v)
		if err != nil {
			return *q, err
		}
		q.Set(dir, val)
	}
	return *q, nil
}

func (qs *QuadStore) getValFromLog(tx Tx, k uint64) (quad.Value, error) {
	if k == 0 {
		return nil, nil
	}
	p, err := qs.getPrimitiveFromLog(tx, k)
	if err != nil {
		return nil, err
	}
	return pquads.UnmarshalValue(p.Value)
}

func (qs *QuadStore) ValueOf(s quad.Value) graph.Value {
	var out Int64Value
	View(qs.db, func(tx Tx) error {
		out = Int64Value(qs.resolveQuadValue(tx, s))
		return nil
	})
	if out == 0 {
		return nil
	}
	return out
}

func (qs *QuadStore) Type() string {
	return qs.db.Type()
}

func (qs *QuadStore) QuadDirection(val graph.Value, d quad.Direction) graph.Value {
	p, ok := val.(*proto.Primitive)
	if !ok {
		return nil
	}
	switch d {
	case quad.Subject:
		return Int64Value(p.Subject)
	case quad.Predicate:
		return Int64Value(p.Predicate)
	case quad.Object:
		return Int64Value(p.Object)
	case quad.Label:
		if p.Label == 0 {
			return nil
		}
		return Int64Value(p.Label)
	}
	return nil
}

func (qs *QuadStore) getPrimitive(val Int64Value) (*proto.Primitive, bool) {
	if val == 0 {
		return nil, false
	}
	var p *proto.Primitive
	err := View(qs.db, func(tx Tx) error {
		var err error
		p, err = qs.getPrimitiveFromLog(tx, uint64(val))
		return err
	})
	if err != nil {
		clog.Errorf("error getting primitive %d: %s", val, err)
		return p, false
	}
	return p, true
}

type Int64Value uint64

func (v Int64Value) Key() interface{} { return v }
