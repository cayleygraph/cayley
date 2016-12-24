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

	"github.com/boltdb/bolt"
	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"

	"github.com/cayleygraph/cayley/graph"
)

func init() {
	graph.RegisterQuadStore(QuadStoreType, graph.QuadStoreRegistration{
		NewFunc:           newQuadStore,
		NewForRequestFunc: nil,
		UpgradeFunc:       nil,
		InitFunc:          createNewBolt,
		IsPersistent:      true,
	})
}

var (
	errNoBucket = errors.New("bolt2: bucket is missing")
)

const localFillPercent = 0.7

const (
	QuadStoreType     = "bolt2"
	latestDataVersion = 1
	nilDataVersion    = 1
	notFound          = 0
)

type QuadStore struct {
	db      *bolt.DB
	path    string
	open    bool
	size    int64
	horizon int64
	version int64
}

func createNewBolt(path string, _ graph.Options) error {
	db, err := bolt.Open(path, 0600, nil)
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
	db, err := bolt.Open(path, 0600, nil)
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
	err = qs.getMetadata()
	if err == errNoBucket {
		return nil, errors.New("bolt: quadstore has not been initialised")
	} else if err != nil {
		return nil, err
	}
	if qs.version != latestDataVersion {
		return nil, errors.New("bolt: data version is out of date. Run cayleyupgrade for your config to update the data.")
	}
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

func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(iterator.Identity)
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
	if v, ok := k.(Int64Node); ok {
		if v == 0 {
			if clog.V(2) {
				clog.Infof("k was 0")
			}
			return nil
		}
		panic("todo, lookup the value")
	}
	if v, ok := k.(Int64Link); ok {
		if v == 0 {
			if clog.V(2) {
				clog.Infof("k was 0")
			}
			return nil
		}
		panic("todo, lookup the link")
	}
	panic("unknown type of graph.Value; not meant for this quadstore")
}

func (qs *QuadStore) Quad(k graph.Value) quad.Quad {
	panic("todo")
}

func (qs *QuadStore) ValueOf(s quad.Value) graph.Value {
	panic("todo")
}

func (qs *QuadStore) QuadDirection(val graph.Value, d quad.Direction) graph.Value {
	p, ok := qs.getPrimitive(val)
	if !ok {
		return Int64Node(0)
	}
	switch d {
	case quad.Subject:
		return Int64Node(p.Subject)
	case quad.Predicate:
		return Int64Node(p.Predicate)
	case quad.Object:
		return Int64Node(p.Object)
	case quad.Label:
		return Int64Node(p.Label)
	}
	return Int64Node(0)
}

func (qs *QuadStore) getPrimitive(val graph.Value) (graph.Primitive, bool) {
	var v graph.Primitive
	index := valToUInt64(val)
	if index == 0 {
		return v, false
	}
	panic("todo:unmarshal")
}

type Int64Node uint64

func (Int64Node) IsNode() bool { return true }

type Int64Link uint64

func (Int64Link) IsNode() bool { return false }

func valToUInt64(val graph.Value) uint64 {
	if v, ok := val.(Int64Node); ok {
		return uint64(v)
	}
	if v, ok := val.(Int64Link); ok {
		return uint64(v)
	}
	return 0
}
