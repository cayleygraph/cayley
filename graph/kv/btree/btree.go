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

package btree

import (
	"bytes"
	"context"
	"io"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/kv"
)

func init() {
	kv.Register(Type, kv.Registration{
		NewFunc:      Create,
		InitFunc:     Create,
		IsPersistent: false,
	})
}

const (
	Type = "btree"
)

func Create(path string, _ graph.Options) (kv.BucketKV, error) {
	return New(), nil
}

func New() *DB {
	return &DB{m: make(map[string]*Tree)}
}

type DB struct {
	m map[string]*Tree
}

func (db *DB) Type() string {
	return Type
}
func (db *DB) Close() error {
	return nil
}
func (db *DB) Tx(update bool) (kv.BucketTx, error) {
	return &Tx{db: db, ro: !update}, nil
}

type Tx struct {
	db  *DB
	ro  bool
	err error
}

func clone(p []byte) []byte {
	if p == nil {
		return nil
	}
	b := make([]byte, len(p))
	copy(b, p)
	return b
}

func (tx *Tx) Get(keys []kv.BucketKey) ([][]byte, error) {
	vals := make([][]byte, len(keys))
	for i, k := range keys {
		if t := tx.db.m[string(k.Bucket)]; t != nil {
			if v, ok := t.Get(k.Key); ok {
				vals[i] = clone(v)
			}
		}
	}
	return vals, nil
}

func (tx *Tx) Commit() error {
	return nil
}
func (tx *Tx) Rollback() error {
	return nil
}
func (tx *Tx) Bucket(name []byte) kv.Bucket {
	t := tx.db.m[string(name)]
	if t == nil && tx.ro {
		return &Bucket{err: kv.ErrNoBucket}
	}
	if t == nil {
		t = TreeNew(bytes.Compare)
		tx.db.m[string(name)] = t
	}
	return &Bucket{tree: t}
}

type Bucket struct {
	tree *Tree
	err  error
}

func (b *Bucket) Get(keys [][]byte) ([][]byte, error) {
	if b.err != nil {
		return nil, b.err
	} else if b.tree == nil {
		return nil, kv.ErrNotFound
	}
	vals := make([][]byte, len(keys))
	for i, k := range keys {
		if v, ok := b.tree.Get(k); ok {
			vals[i] = clone(v)
		}
	}
	return vals, nil
}
func (b *Bucket) Put(k, v []byte) error {
	if b.err != nil {
		return b.err
	}
	b.tree.Set(clone(k), clone(v))
	return nil
}
func (b *Bucket) Del(k []byte) error {
	if b.err != nil {
		return b.err
	}
	b.tree.Delete(k)
	return nil
}
func (b *Bucket) Scan(pref []byte) kv.KVIterator {
	return &Iterator{b: b, pref: pref}
}

type Iterator struct {
	b    *Bucket
	pref []byte
	e    *Enumerator
	k, v []byte
}

func (it *Iterator) Next(ctx context.Context) bool {
	if it.b == nil {
		return false
	} else if it.b.err != nil {
		return false
	}
	if it.e == nil {
		it.e, _ = it.b.tree.Seek(it.pref)
	}
	k, v, err := it.e.Next()
	if err == io.EOF {
		return false
	} else if !bytes.HasPrefix(k, it.pref) {
		return false
	}
	it.k, it.v = k, v
	return true
}
func (it *Iterator) Key() []byte { return it.k }
func (it *Iterator) Val() []byte { return it.v }
func (it *Iterator) Err() error {
	if it.b == nil {
		return nil
	}
	return it.b.err
}
func (it *Iterator) Close() error {
	if it.e != nil {
		it.e.Close()
		it.e = nil
	}
	return it.Err()
}
