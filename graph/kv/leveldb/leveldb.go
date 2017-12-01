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

package leveldb

import (
	"context"
	"fmt"
	"os"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/kv"
)

func init() {
	kv.Register(Type, kv.Registration{
		NewFunc:      Open,
		InitFunc:     Create,
		IsPersistent: true,
	})
}

const (
	Type = "leveldb"
)

func newDB(d *leveldb.DB, m graph.Options) *DB {
	db := &DB{
		DB: d,
		wo: &opt.WriteOptions{},
	}
	nosync, _, _ := m.BoolKey("nosync")
	db.wo.Sync = !nosync
	return db
}

func Create(path string, m graph.Options) (kv.BucketKV, error) {
	err := os.MkdirAll(path, 0700)
	if err != nil {
		return nil, err
	}
	db, err := leveldb.OpenFile(path, &opt.Options{
		ErrorIfExist: true,
	})
	if os.IsExist(err) {
		return nil, graph.ErrDatabaseExists
	} else if err != nil {
		return nil, err
	}
	return kv.FromFlat(newDB(db, m)), nil
}

func Open(path string, m graph.Options) (kv.BucketKV, error) {
	db, err := leveldb.OpenFile(path, &opt.Options{
		ErrorIfMissing: true,
	})
	if err != nil {
		return nil, err
	}
	return kv.FromFlat(newDB(db, m)), nil
}

type DB struct {
	DB *leveldb.DB
	wo *opt.WriteOptions
	ro *opt.ReadOptions
}

func (db *DB) Type() string {
	return Type
}
func (db *DB) Close() error {
	return db.DB.Close()
}
func (db *DB) Tx(update bool) (kv.FlatTx, error) {
	tx := &Tx{db: db}
	var err error
	if update {
		tx.tx, err = db.DB.OpenTransaction()
	} else {
		tx.sn, err = db.DB.GetSnapshot()
	}
	if err != nil {
		return nil, err
	}
	return tx, nil
}

type Tx struct {
	db  *DB
	sn  *leveldb.Snapshot
	tx  *leveldb.Transaction
	err error
}

func (tx *Tx) Commit() error {
	if tx.err != nil {
		return tx.err
	}
	if tx.tx != nil {
		tx.err = tx.tx.Commit()
		return tx.err
	}
	tx.sn.Release()
	return tx.err
}
func (tx *Tx) Rollback() error {
	if tx.tx != nil {
		tx.tx.Discard()
	} else {
		tx.sn.Release()
	}
	return tx.err
}
func (tx *Tx) Get(keys [][]byte) ([][]byte, error) {
	vals := make([][]byte, len(keys))
	var err error
	var get func(k []byte, ro *opt.ReadOptions) ([]byte, error)
	if tx.tx != nil {
		get = tx.tx.Get
	} else {
		get = tx.sn.Get
	}
	for i, k := range keys {
		vals[i], err = get(k, tx.db.ro)
		if err == leveldb.ErrNotFound {
			vals[i] = nil
		} else if err != nil {
			return nil, err
		}
	}
	return vals, nil
}
func (tx *Tx) Put(k, v []byte) error {
	if tx.tx == nil {
		return fmt.Errorf("put on ro tx")
	}
	return tx.tx.Put(k, v, tx.db.wo)
}
func (tx *Tx) Del(k []byte) error {
	if tx.tx == nil {
		return fmt.Errorf("del on ro tx")
	}
	return tx.tx.Delete(k, tx.db.wo)
}
func (tx *Tx) Scan(pref []byte) kv.KVIterator {
	r, ro := util.BytesPrefix(pref), tx.db.ro
	var it iterator.Iterator
	if tx.tx != nil {
		it = tx.tx.NewIterator(r, ro)
	} else {
		it = tx.sn.NewIterator(r, ro)
	}
	return &Iterator{it: it, first: true}
}

type Iterator struct {
	it    iterator.Iterator
	first bool
}

func (it *Iterator) Next(ctx context.Context) bool {
	if it.first {
		it.first = false
		return it.it.First()
	}
	return it.it.Next()
}
func (it *Iterator) Key() []byte { return it.it.Key() }
func (it *Iterator) Val() []byte { return it.it.Value() }
func (it *Iterator) Err() error {
	return it.it.Error()
}
func (it *Iterator) Close() error {
	it.it.Release()
	return it.Err()
}
