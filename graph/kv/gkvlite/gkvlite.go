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

package gkvlite

import (
	"bytes"
	"context"
	"os"
	"path"
	"sync"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/kv"
	"github.com/cbhvn/gkvlite"
)

const (
	Type = "gkvlite"
)

func init() {
	kv.Register(Type, kv.Registration{
		NewFunc:      Open,
		InitFunc:     Create,
		IsPersistent: true,
	})
}

func getStoreFile(folder string) string {
	return path.Join(folder, "store.gkvlite")
}

func Create(path string, m graph.Options) (kv.BucketKV, error) {
	var f *os.File
	if path != "" {
		err := os.MkdirAll(path, 0700)
		if err != nil {
			return nil, err
		}
		f, err = os.Create(getStoreFile(path))
		if err != nil {
			return nil, err
		}
	}
	return newStore(f)
}

func Open(path string, m graph.Options) (kv.BucketKV, error) {
	var f *os.File
	if path != "" {
		err := os.MkdirAll(path, 0700)
		if err != nil {
			return nil, err
		}
		f, err = os.Open(getStoreFile(path))
		if err != nil {
			return nil, err
		}
	}
	return newStore(f)
}

func getColl(s *gkvlite.Store) *gkvlite.Collection {
	return s.SetCollection("cayleygraph", nil)
}

func newStore(f *os.File) (kv.BucketKV, error) {
	store, err := gkvlite.NewStore(f)
	if err != nil {
		return nil, err
	}
	c := getColl(store)
	db := &DB{
		store: store,
		file:  f,
		c:     c,
	}

	return kv.FromFlat(db), nil
}

type DB struct {
	mu    sync.RWMutex
	store *gkvlite.Store
	file  *os.File
	c     *gkvlite.Collection

	isClosed bool
}

func (db *DB) Type() string {
	return Type
}

func (db *DB) Close() error {
	if db.store == nil || db.isClosed {
		return nil
	}
	db.isClosed = true
	db.store.Close()
	if db.file != nil {
		return db.file.Close()
	}

	return nil
}

func (db *DB) Tx(update bool) (kv.FlatTx, error) {
	return &Tx{db, false}, nil
}

type Tx struct {
	db    *DB
	dirty bool
}

func (tx *Tx) ensureNil() {
	tx.db = nil
}

func (tx *Tx) Commit(ctx context.Context) error {
	if tx.db == nil {
		return nil
	}
	if tx.db.file == nil {
		return nil
	}
	if !tx.dirty {
		return nil
	}
	tx.db.mu.Lock()
	defer tx.ensureNil()
	defer tx.db.mu.Unlock()
	err := tx.db.store.Flush()
	return err
}

func (tx *Tx) Rollback() error {
	if tx.db == nil {
		return nil
	}
	if tx.db.file == nil {
		return nil
	}
	if !tx.dirty {
		return nil
	}
	tx.db.mu.Lock()
	defer tx.ensureNil()
	defer tx.db.mu.Unlock()
	err := tx.db.store.Flush()
	if err == nil {
		err = tx.db.store.FlushRevert()
	}
	tx.db.c = getColl(tx.db.store)
	return err
}

func (tx *Tx) Get(ctx context.Context, keys [][]byte) ([][]byte, error) {
	if tx.db != nil {
		tx.db.mu.RLock()
		defer tx.db.mu.RUnlock()
	}
	vals := make([][]byte, len(keys))
	for i, k := range keys {
		val, err := tx.db.c.Get(k)
		if err != nil {
			continue
		}
		vals[i] = val
	}
	return vals, nil
}

func (tx *Tx) Put(k, v []byte) error {
	if tx.db != nil {
		tx.db.mu.Lock()
		defer tx.db.mu.Unlock()
	}
	tx.dirty = true
	return tx.db.c.Set(k, v)
}

func (tx *Tx) Del(k []byte) error {
	if tx.db != nil {
		tx.db.mu.Lock()
		defer tx.db.mu.Unlock()
	}
	tx.dirty = true
	_, err := tx.db.c.Delete(k)
	return err
}

func (tx *Tx) Scan(pref []byte) kv.KVIterator {
	if tx.db != nil {
		tx.db.mu.RLock()
		defer tx.db.mu.RUnlock()
	}
	it := tx.db.c.IterateAscend(pref, true)
	return &Iterator{iter: it, pref: pref}
}

type Iterator struct {
	iter gkvlite.ItemIterator
	pref []byte
	done bool
}

func (it *Iterator) Next(ctx context.Context) bool {
	if it.done {
		return false
	}
	it.done = !it.iter.Next()
	if it.done {
		return false
	}
	it.done = !bytes.HasPrefix(it.iter.Result().Key, it.pref)
	return !it.done
}

func (it *Iterator) Key() []byte {
	return it.iter.Result().Key
}

func (it *Iterator) Val() []byte {
	return it.iter.Result().Val
}

func (it *Iterator) Err() error {
	return it.iter.Err()
}

func (it *Iterator) Close() error {
	it.iter.Close()
	return it.Err()
}
