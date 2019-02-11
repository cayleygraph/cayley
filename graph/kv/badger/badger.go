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
	"errors"
	"os"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/kv"
)

const (
	Type = "badger"
)

var (
	ErrTxNotWritable = errors.New("Transaction is read-only")
)

func init() {
	// badger.SetLogger(Logger{})
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

	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	opts.ValueLogLoadingMode = options.FileIO
	opts.TableLoadingMode = options.FileIO

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
	tx := &Tx{update: update, db: db}
	tx.txn = db.DB.NewTransaction(update)
	return tx, nil
}

type Tx struct {
	db     *DB
	txn    *badger.Txn
	err    error
	update bool
}

func (tx *Tx) Commit(ctx context.Context) error {
	if tx.err != nil {
		return tx.err
	}
	if !tx.update {
		return nil
	}
	tx.err = tx.txn.Commit(nil)
	return tx.err
}
func (tx *Tx) Rollback() error {
	tx.txn.Discard()
	return tx.err
}
func (tx *Tx) Get(ctx context.Context, keys [][]byte) ([][]byte, error) {
	vals := make([][]byte, len(keys))
	for i, k := range keys {
		v, err := tx.txn.Get(k)
		if err != nil && err != badger.ErrKeyNotFound {
			return nil, err
		}
		if v == nil {
			vals[i] = nil
			continue
		}
		val, err := v.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		vals[i] = val
	}
	return vals, nil
}
func (tx *Tx) Put(k, v []byte) error {
	if !tx.update {
		return ErrTxNotWritable
	}
	return tx.txn.Set(k, v)
}
func (tx *Tx) Del(k []byte) error {
	if !tx.update {
		return ErrTxNotWritable
	}
	return tx.txn.Delete(k)
}
func (tx *Tx) Scan(pref []byte) kv.KVIterator {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.PrefetchSize = 100
	it := tx.txn.NewIterator(opts)
	return &Iterator{iter: it, first: true, pref: pref}
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
		if it.pref != nil {
			it.iter.Seek(it.pref)
			return it.iter.ValidForPrefix(it.pref)
		} else {
			it.iter.Rewind()
			return it.iter.Valid()
		}
	}
	if it.pref != nil {
		it.iter.Next()
		return it.iter.ValidForPrefix(it.pref)
	} else {
		it.iter.Next()
		return it.iter.Valid()
	}
}
func (it *Iterator) Key() []byte { return it.iter.Item().Key() }
func (it *Iterator) Val() []byte {
	val, err := it.iter.Item().ValueCopy(nil)
	it.err = err
	return val
}
func (it *Iterator) Err() error {
	return it.err
}
func (it *Iterator) Close() error {
	it.iter.Close()
	return it.err
}

type Logger struct{}

func (Logger) Errorf(s string, i ...interface{})   {}
func (Logger) Infof(s string, i ...interface{})    {}
func (Logger) Warningf(s string, i ...interface{}) {}
