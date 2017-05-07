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
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/kv"
)

func init() {
	graph.RegisterQuadStore(QuadStoreType, graph.QuadStoreRegistration{
		NewFunc:      newQuadStore,
		UpgradeFunc:  nil,
		InitFunc:     createNewBolt,
		IsPersistent: true,
	})
}

const (
	QuadStoreType = "bolt2"
)

func getBoltFile(cfgpath string) string {
	return filepath.Join(cfgpath, "indexes.bolt")
}

func createNewBolt(path string, options graph.Options) error {
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
	return kv.Init(&DB{DB: db}, options)
}

func newQuadStore(path string, options graph.Options) (graph.QuadStore, error) {
	db, err := bolt.Open(getBoltFile(path), 0600, nil)
	if err != nil {
		clog.Errorf("Error, couldn't open! %v", err)
		return nil, err
	}
	// BoolKey returns false on non-existence. IE, Sync by default.
	db.NoSync, _, err = options.BoolKey("nosync")
	if err != nil {
		db.Close()
		return nil, err
	}
	if db.NoSync {
		clog.Infof("Running in nosync mode")
	}
	return kv.New(&DB{DB: db}, options)
}

type DB struct {
	*bolt.DB
}

func (db *DB) Type() string {
	return QuadStoreType
}
func (db *DB) View() (kv.Tx, error) {
	tx, err := db.DB.Begin(false)
	if err != nil {
		return nil, err
	}
	return Tx{tx}, nil
}
func (db *DB) Update() (kv.Tx, error) {
	tx, err := db.DB.Begin(true)
	if err != nil {
		return nil, err
	}
	return Tx{tx}, nil
}

type Tx struct {
	*bolt.Tx
}

func (tx Tx) Bucket(name []byte) kv.Bucket {
	b := tx.Tx.Bucket(name)
	if b == nil {
		return nil
	}
	return Bucket{b}
}
func (tx Tx) CreateBucket(name []byte) (kv.Bucket, error) {
	b, err := tx.Tx.CreateBucket(name)
	if err != nil {
		return nil, err
	}
	return Bucket{b}, nil
}

var _ kv.FillBucket = Bucket{}

type Bucket struct {
	*bolt.Bucket
}

func (b Bucket) SetFillPercent(v float64) {
	b.FillPercent = v
}
