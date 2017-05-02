// Copyright 2016 The Cayley Authors. All rights reserved.
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

package bolt2

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"
	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph/proto"
	"github.com/cayleygraph/cayley/internal/lru"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/quad/pquads"
	boom "github.com/tylertreat/BoomFilters"

	"github.com/cayleygraph/cayley/graph"
)

func init() {
	graph.RegisterQuadStore(QuadStoreType, graph.QuadStoreRegistration{
		NewFunc:      newQuadStore,
		UpgradeFunc:  nil,
		InitFunc:     createNewBolt,
		IsPersistent: true,
	})
}

var (
	errNoBucket = errors.New("bolt2: bucket is missing")
)

const (
	QuadStoreType     = "bolt2"
	latestDataVersion = 1
	nilDataVersion    = 1
	notFound          = 0
)

type QuadStore struct {
	db            *bolt.DB
	path          string
	open          bool
	size          int64
	horizon       int64
	sameAsHorizon int64
	version       int64
	mapBucket     map[string]map[uint64][]uint64
	valueLRU      *lru.Cache
	exists        *boom.DeletableBloomFilter
}

func getBoltFile(cfgpath string) string {
	return filepath.Join(cfgpath, "indexes.bolt")
}

func createNewBolt(path string, _ graph.Options) error {
	err := os.MkdirAll(path, 0700)
	if err != nil {
		return err
	}
	db, err := bolt.Open(getBoltFile(path), 0600, nil)
	if err != nil {
		clog.Errorf("Error: couldn't create Bolt database: %v", err)
		return err
	}
	defer db.Close()
	qs := &QuadStore{}
	qs.db = db
	defer qs.Close()
	err = qs.getMetadata()
	if err != errNoBucket {
		return graph.ErrDatabaseExists
	}
	err = qs.createBuckets()
	if err != nil {
		return err
	}
	err = setVersion(qs.db, latestDataVersion)
	if err != nil {
		return err
	}
	qs.Close()
	return nil
}

func newQuadStore(path string, options graph.Options) (graph.QuadStore, error) {
	var qs QuadStore
	var err error
	db, err := bolt.Open(getBoltFile(path), 0600, nil)
	if err != nil {
		clog.Errorf("Error, couldn't open! %v", err)
		return nil, err
	}
	qs.db = db
	// BoolKey returns false on non-existence. IE, Sync by default.
	qs.db.NoSync, _, err = options.BoolKey("nosync")
	if err != nil {
		return nil, err
	}
	if qs.db.NoSync {
		clog.Infof("Running in nosync mode")
	}
	err = qs.getMetadata()
	if err == errNoBucket {
		return nil, errors.New("bolt: quadstore has not been initialised")
	} else if err != nil {
		return nil, err
	}
	if qs.version != latestDataVersion {
		return nil, errors.New("bolt: data version is out of date. Run cayleyupgrade for your config to update the data.")
	}
	qs.valueLRU = lru.New(2000)
	qs.initBloomFilter()
	return &qs, nil
}

func setVersion(db *bolt.DB, version int64) error {
	return db.Update(func(tx *bolt.Tx) error {
		buf := new(bytes.Buffer)
		err := binary.Write(buf, binary.LittleEndian, version)
		if err != nil {
			clog.Errorf("Couldn't convert version!")
			return err
		}
		b := tx.Bucket(metaBucket)
		werr := b.Put([]byte("version"), buf.Bytes())
		if werr != nil {
			clog.Errorf("Couldn't write version!")
			return werr
		}
		return nil
	})
}

func (qs *QuadStore) Size() int64 {
	return qs.size
}

func (qs *QuadStore) Close() error {
	qs.db.Update(func(tx *bolt.Tx) error {
		return qs.writeHorizonAndSize(tx)
	})
	err := qs.db.Close()
	qs.open = false
	return err
}

func (qs *QuadStore) getMetadata() error {
	err := qs.db.View(func(tx *bolt.Tx) error {
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
	return err
}

func getInt64ForMetaKey(tx *bolt.Tx, key string, empty int64) (int64, error) {
	return getInt64ForKey(tx, metaBucket, key, empty)
}

func getInt64ForKey(tx *bolt.Tx, bucket []byte, key string, empty int64) (int64, error) {
	var out int64
	b := tx.Bucket(bucket)
	if b == nil {
		return empty, errNoBucket
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
		err := qs.db.View(func(tx *bolt.Tx) error {
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
	key := k.(*proto.Primitive)
	err := qs.db.View(func(tx *bolt.Tx) error {
		var err error
		v, err = qs.primitiveToQuad(tx, key)
		return err
	})
	if err != nil {
		clog.Errorf("error fetching quad %d: %s", key, err)
	}
	return v
}

func (qs *QuadStore) primitiveToQuad(tx *bolt.Tx, p *proto.Primitive) (quad.Quad, error) {
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

func (qs *QuadStore) getValFromLog(tx *bolt.Tx, k uint64) (quad.Value, error) {
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
	qs.db.View(func(tx *bolt.Tx) error {
		out = Int64Value(qs.resolveQuadValue(tx, s))
		return nil
	})
	return out
}

func (qs *QuadStore) Type() string {
	return QuadStoreType
}

func (qs *QuadStore) QuadDirection(val graph.Value, d quad.Direction) graph.Value {
	p, ok := val.(*proto.Primitive)
	if !ok {
		return Int64Value(0)
	}
	switch d {
	case quad.Subject:
		return Int64Value(p.Subject)
	case quad.Predicate:
		return Int64Value(p.Predicate)
	case quad.Object:
		return Int64Value(p.Object)
	case quad.Label:
		return Int64Value(p.Label)
	}
	return Int64Value(0)
}

func (qs *QuadStore) getPrimitive(val Int64Value) (*proto.Primitive, bool) {
	if val == 0 {
		return nil, false
	}
	var p *proto.Primitive
	err := qs.db.View(func(tx *bolt.Tx) error {
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
