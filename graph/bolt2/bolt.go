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
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"

	"github.com/cayleygraph/cayley/clog"
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
	Type = "bolt2"
)

func getBoltFile(cfgpath string) string {
	return filepath.Join(cfgpath, "indexes.bolt")
}

func Create(path string, _ graph.Options) (kv.BucketKV, error) {
	err := os.MkdirAll(path, 0700)
	if err != nil {
		return nil, err
	}
	db, err := bolt.Open(getBoltFile(path), 0600, nil)
	if err != nil {
		clog.Errorf("Error: couldn't create Bolt database: %v", err)
		return nil, err
	}
	return &DB{DB: db}, nil
}

func Open(path string, opt graph.Options) (kv.BucketKV, error) {
	db, err := bolt.Open(getBoltFile(path), 0600, nil)
	if err != nil {
		clog.Errorf("Error, couldn't open! %v", err)
		return nil, err
	}
	// BoolKey returns false on non-existence. IE, Sync by default.
	db.NoSync, _, err = opt.BoolKey("nosync")
	if err != nil {
		db.Close()
		return nil, err
	}
	if db.NoSync {
		clog.Infof("Running in nosync mode")
	}
	return &DB{DB: db}, nil
}

type DB struct {
	DB *bolt.DB
}

func (db *DB) Type() string {
	return Type
}

func (db *DB) Close() error {
	return db.DB.Close()
}

func (db *DB) Tx(update bool) (kv.BucketTx, error) {
	tx, err := db.DB.Begin(update)
	if err != nil {
		return nil, err
	}
	return &Tx{Tx: tx}, nil
}

type Tx struct {
	Tx *bolt.Tx
}

func (tx *Tx) Commit() error {
	return tx.Tx.Commit()
}
func (tx *Tx) Rollback() error {
	return tx.Tx.Rollback()
}
func (tx *Tx) Bucket(name []byte, op kv.Op) (kv.Bucket, error) {
	var (
		b   *bolt.Bucket
		err error
	)
	switch op {
	case kv.OpGet:
		b = tx.Tx.Bucket(name)
		if b == nil {
			return nil, kv.ErrNoBucket
		}
	case kv.OpCreate:
		b, err = tx.Tx.CreateBucket(name)
		if err == bolt.ErrBucketExists {
			return nil, kv.ErrBucketExists
		}
	case kv.OpUpsert:
		b, err = tx.Tx.CreateBucketIfNotExists(name)
	default:
		return nil, fmt.Errorf("unsupported operation")
	}
	if err != nil {
		return nil, err
	}
	return &Bucket{b}, nil
}

var _ kv.FillBucket = (*Bucket)(nil)

type Bucket struct {
	Bucket *bolt.Bucket
}

func (b *Bucket) Get(k []byte) ([]byte, error) {
	v := b.Bucket.Get(k)
	if v == nil {
		return nil, kv.ErrNotFound
	}
	return v, nil
}
func (b *Bucket) Put(k, v []byte) error { return b.Bucket.Put(k, v) }
func (b *Bucket) Del(k []byte) error    { return b.Bucket.Delete(k) }
func (b *Bucket) Scan(pref []byte) kv.KVIterator {
	return &Iterator{b: b, pref: pref}
}

type Iterator struct {
	b    *Bucket
	pref []byte
	c    *bolt.Cursor
	k, v []byte
}

func (it *Iterator) Next(ctx context.Context) bool {
	if it.b == nil {
		return false
	}
	if it.c == nil {
		it.c = it.b.Bucket.Cursor()
		if len(it.pref) == 0 {
			it.k, it.v = it.c.First()
		} else {
			it.k, it.v = it.c.Seek(it.pref)
		}
	} else {
		it.k, it.v = it.c.Next()
	}
	ok := it.k != nil && bytes.HasPrefix(it.k, it.pref)
	if !ok {
		it.b = nil
	}
	return ok
}
func (it *Iterator) Key() []byte  { return it.k }
func (it *Iterator) Val() []byte  { return it.v }
func (it *Iterator) Err() error   { return nil }
func (it *Iterator) Close() error { return nil }

func (b *Bucket) SetFillPercent(v float64) {
	b.Bucket.FillPercent = v
}
