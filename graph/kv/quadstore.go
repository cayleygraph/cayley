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

type Registration struct {
	NewFunc      NewFunc
	InitFunc     InitFunc
	IsPersistent bool
}

type InitFunc func(string, graph.Options) (BucketKV, error)
type NewFunc func(string, graph.Options) (BucketKV, error)

func Register(name string, r Registration) {
	graph.RegisterQuadStore(name, graph.QuadStoreRegistration{
		InitFunc: func(addr string, opt graph.Options) error {
			kv, err := r.InitFunc(addr, opt)
			if err != nil {
				return err
			}
			defer kv.Close()
			if err = Init(kv, opt); err != nil {
				return err
			}
			return kv.Close()
		},
		NewFunc: func(addr string, opt graph.Options) (graph.QuadStore, error) {
			kv, err := r.NewFunc(addr, opt)
			if err != nil {
				return nil, err
			}
			return New(kv, opt)
		},
		IsPersistent: r.IsPersistent,
	})
}

const (
	latestDataVersion = 1
	nilDataVersion    = 1
	notFound          = 0
)

var _ graph.BatchQuadStore = (*QuadStore)(nil)

type QuadStore struct {
	db   BucketKV
	path string

	writer    sync.Mutex
	valueLRU  *lru.Cache
	mapBucket map[string]map[uint64][]uint64
	exists    *boom.DeletableBloomFilter

	mu            sync.RWMutex
	size          int64
	horizon       int64
	sameAsHorizon int64
	version       int64

	bufLock  sync.Mutex
	hashBuf  []byte
	bloomBuf []byte
}

func Init(kv BucketKV, opt graph.Options) error {
	qs := &QuadStore{db: kv}
	if err := qs.getMetadata(); err != ErrNoBucket {
		return graph.ErrDatabaseExists
	}
	upfront, _, _ := opt.BoolKey("upfront")
	if err := qs.createBuckets(upfront); err != nil {
		return err
	}
	if err := setVersion(qs.db, latestDataVersion); err != nil {
		return err
	}
	return nil
}

func New(kv BucketKV, _ graph.Options) (graph.QuadStore, error) {
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
	qs.bloomBuf = make([]byte, 3*8)
	qs.hashBuf = make([]byte, quad.HashSize)
	qs.initBloomFilter()
	return qs, nil
}

func setVersion(kv BucketKV, version int64) error {
	return Update(kv, func(tx BucketTx) error {
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
	err := Update(qs.db, func(tx BucketTx) error {
		return qs.writeHorizonAndSize(tx, -1, -1)
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
	return View(qs.db, func(tx BucketTx) error {
		b := tx.Bucket(metaBucket)
		var err error
		vals, err := b.Get([][]byte{
			[]byte("version"),
			[]byte("size"),
			[]byte("sameAsHorizon"),
			[]byte("horizon"),
		})
		if err == ErrNotFound {
			return ErrNoBucket
		} else if err != nil {
			return err
		} else if vals[0] == nil {
			return ErrNoBucket
		}

		qs.version, err = asInt64(vals[0], nilDataVersion)
		if err != nil {
			return err
		}
		qs.size, err = asInt64(vals[1], 0)
		if err != nil {
			return err
		}
		qs.sameAsHorizon, err = asInt64(vals[2], 0)
		if err != nil {
			return err
		}
		qs.horizon, err = asInt64(vals[3], 0)
		if err != nil {
			return err
		}
		return nil
	})
}

func asInt64(b []byte, empty int64) (int64, error) {
	if len(b) == 0 {
		return empty, nil
	} else if len(b) != 8 {
		return 0, fmt.Errorf("unexpected int size: %d", len(b))
	}
	v := int64(binary.LittleEndian.Uint64(b))
	return v, nil
}

func (qs *QuadStore) Horizon() graph.PrimaryKey {
	qs.mu.RLock()
	defer qs.mu.RUnlock()
	return graph.NewSequentialKey(qs.horizon)
}

func (qs *QuadStore) ValuesOf(vals []graph.Value) ([]quad.Value, error) {
	out := make([]quad.Value, len(vals))
	var (
		inds []int
		refs []uint64
	)
	for i, v := range vals {
		if v == nil {
			continue
		} else if pv, ok := v.(graph.PreFetchedValue); ok {
			out[i] = pv.NameOf()
			continue
		}
		switch v := v.(type) {
		case Int64Value:
			if v == 0 {
				continue
			}
			inds = append(inds, i)
			refs = append(refs, uint64(v))
		default:
			return out, fmt.Errorf("unknown type of graph.Value; not meant for this quadstore. apparently a %#v", v)
		}
	}
	if len(refs) == 0 {
		return out, nil
	}
	prim, err := qs.getPrimitives(refs)
	if err != nil {
		return out, err
	}
	var last error
	for i, p := range prim {
		if !p.IsNode() {
			continue
		}
		qv, err := pquads.UnmarshalValue(p.Value)
		if err != nil {
			last = err
			continue
		}
		out[inds[i]] = qv
	}
	return out, last
}
func (qs *QuadStore) NameOf(v graph.Value) quad.Value {
	vals, err := qs.ValuesOf([]graph.Value{v})
	if err != nil {
		clog.Errorf("error getting NameOf %d: %s", v, err)
		return nil
	}
	return vals[0]
}

func (qs *QuadStore) Quad(k graph.Value) quad.Quad {
	var v quad.Quad
	key, ok := k.(*proto.Primitive)
	if !ok {
		clog.Errorf("passed value was not a quad primitive: %T", k)
		return quad.Quad{}
	}
	err := View(qs.db, func(tx BucketTx) error {
		var err error
		v, err = qs.primitiveToQuad(tx, key)
		return err
	})
	if err != nil {
		clog.Errorf("error fetching quad %#v: %s", key, err)
	}
	return v
}

func (qs *QuadStore) primitiveToQuad(tx BucketTx, p *proto.Primitive) (quad.Quad, error) {
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

func (qs *QuadStore) getValFromLog(tx BucketTx, k uint64) (quad.Value, error) {
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
	_ = View(qs.db, func(tx BucketTx) error {
		v, err := qs.resolveQuadValue(tx, s)
		out = Int64Value(v)
		return err
	})
	if out == 0 {
		return nil
	}
	return out
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

func (qs *QuadStore) getPrimitives(vals []uint64) ([]*proto.Primitive, error) {
	tx, err := qs.db.Tx(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return qs.getPrimitivesFromLog(tx, vals)
}

type Int64Value uint64

func (v Int64Value) Key() interface{} { return v }
