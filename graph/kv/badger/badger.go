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

package badger

import (
	"context"
	"os"

	"github.com/dgraph-io/badger"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/kv"
)

const (
	Type = "badger"
)

var (
	DatastoreOpts = badger.DefaultOptions
	IteratorOpts  = badger.IteratorOptions{PrefetchValues: false}
)

func init() {
	kv.Register(Type, kv.Registration{
		NewFunc:      Create,
		InitFunc:     Create,
		IsPersistent: true,
	})
}

func Create(path string, m graph.Options) (kv.BucketKV, error) {
	err := os.MkdirAll(path, 0700)
	if err != nil {
		return nil, err
	}

	opts := DatastoreOpts
	opts.Dir = path
	opts.ValueDir = path

	store, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	db := &DB{
		DB: store,
	}
	return kv.FromFlat(db), nil
}

type DB struct {
	DB       *badger.DB
	isClosed bool
}

func (db *DB) Type() string {
	return Type
}

func (db *DB) Close() error {
	if db.DB == nil || db.isClosed {
		return nil
	}
	db.isClosed = true
	return db.DB.Close()
}

func (db *DB) Tx(update bool) (kv.FlatTx, error) {
	tx := &Tx{}
	tx.txn = db.DB.NewTransaction(update)
	return tx, nil
}

type Tx struct {
	txn *badger.Txn
}

func (tx *Tx) Commit(ctx context.Context) error {
	return tx.txn.Commit()
}

func (tx *Tx) Rollback() error {
	tx.txn.Discard()
	return nil
}

func (tx *Tx) Get(ctx context.Context, keys [][]byte) ([][]byte, error) {
	vals := make([][]byte, len(keys))
	for i, k := range keys {
		item, err := tx.txn.Get(k)
		if err == badger.ErrKeyNotFound {
			continue
		} else if err != nil {
			return nil, err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		vals[i] = val
	}
	return vals, nil
}

func (tx *Tx) Put(k, v []byte) error {
	return tx.txn.Set(k, v)
}

func (tx *Tx) Del(k []byte) error {
	return tx.txn.Delete(k)
}

func (tx *Tx) Scan(pref []byte) kv.KVIterator {
	it := tx.txn.NewIterator(IteratorOpts)
	return &Iterator{iter: it, pref: pref, first: true}
}

type Iterator struct {
	iter  *badger.Iterator
	first bool
	pref  []byte
	err   error
}

func (it *Iterator) Next(ctx context.Context) bool {
	if it.first {
		it.first = false
		it.iter.Seek(it.pref)
	} else {
		it.iter.Next()
	}
	if len(it.pref) != 0 {
		return it.iter.ValidForPrefix(it.pref)
	}
	return it.iter.Valid()
}

func (it *Iterator) Key() []byte {
	return it.iter.Item().Key()
}

func (it *Iterator) Val() []byte {
	v, err := it.iter.Item().ValueCopy(nil)
	it.err = err
	return v
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Close() error {
	it.iter.Close()
	return it.Err()
}
