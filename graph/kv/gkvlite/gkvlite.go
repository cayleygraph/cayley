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
	"context"
	"os"
	"path"

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

func Create(folder string, m graph.Options) (kv.BucketKV, error) {
	var f *os.File
	if folder != "" {
		err := os.MkdirAll(folder, 0700)
		if err != nil {
			return nil, err
		}
		p := path.Join(folder, "store.gkvlite")
		f, err = os.Create(p)
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
		f, err = os.Open(path)
		if err != nil {
			return nil, err
		}
	}

	return newStore(f)
}

func newStore(f *os.File) (kv.BucketKV, error) {

	store, err := gkvlite.NewStore(f)
	c := store.SetCollection("cayleygraph", nil)

	if err != nil {
		return nil, err
	}
	db := &DB{
		store: store,
		file:  f,
		c:     c,
	}
	return kv.FromFlat(db), nil
}

type DB struct {
	store    *gkvlite.Store
	isClosed bool
	file     *os.File
	c        *gkvlite.Collection
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
	return &Tx{db, nil}, nil
}

type optype int

const (
	_ = iota
	put
	del
)

type op struct {
	t optype
	k []byte
	v []byte
}

type Tx struct {
	db  *DB
	ops []op
}

func (tx *Tx) Commit(ctx context.Context) error {
	defer func() {
		tx.ops = nil
	}()
	for _, op := range tx.ops {
		switch op.t {
		case put:
			err := tx.db.c.Set(op.k, op.v)
			if err != nil {
				return err
			}
		case del:
			_, err := tx.db.c.Delete(op.k)
			if err != nil {
				return err
			}
		}
	}
	if tx.db.file == nil {
		return nil
	}
	err := tx.db.store.Flush()
	return err
}

func (tx *Tx) Rollback() error {
	tx.ops = nil
	return nil
}

func (tx *Tx) Get(ctx context.Context, keys [][]byte) ([][]byte, error) {
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
	tx.ops = append(tx.ops, op{put, k, v})
	return nil
}

func (tx *Tx) Del(k []byte) error {
	tx.ops = append(tx.ops, op{del, k, nil})
	return nil
}

func (tx *Tx) Scan(pref []byte) kv.KVIterator {
	it := tx.db.c.IterateAscend(pref, true)
	return &Iterator{iter: it, pref: pref, first: true}
}

type Iterator struct {
	iter  gkvlite.ItemIterator
	first bool
	pref  []byte
	err   error
}

func (it *Iterator) Next(ctx context.Context) bool {
	return it.iter.Next()
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
