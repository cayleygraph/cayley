// Copyright 2015 The Cayley Authors. All rights reserved.
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

package bolt

import (
	"encoding/json"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/proto"
)

const latestDataVersion = 3
const nilDataVersion = 1

type upgradeFunc func(*bolt.DB) error

var migrateFunctions = []upgradeFunc{
	nil,
	upgrade1To2,
	upgrade2To3,
}

func upgradeBolt(path string, opts graph.Options) error {
	db, err := bolt.Open(path, 0600, nil)
	defer db.Close()

	if err != nil {
		clog.Errorf("Error, couldn't open! %v", err)
		return err
	}
	var version int64
	err = db.View(func(tx *bolt.Tx) error {
		version, err = getInt64ForMetaKey(tx, "version", nilDataVersion)
		return err
	})
	if err != nil {
		clog.Errorf("error: %v", err)
		return err
	}

	if version == latestDataVersion {
		fmt.Printf("Already at latest version: %d\n", latestDataVersion)
		return nil
	}

	if version > latestDataVersion {
		err := fmt.Errorf("Unknown data version: %d -- upgrade this tool", version)
		clog.Errorf("error: %v", err)
		return err
	}

	for i := version; i < latestDataVersion; i++ {
		err := migrateFunctions[i](db)
		if err != nil {
			return err
		}
		setVersion(db, i+1)
	}

	return nil
}

type v1ValueData struct {
	Name string
	Size int64
}

type v1IndexEntry struct {
	History []int64
}

func upgrade1To2(db *bolt.DB) error {
	fmt.Println("Upgrading v1 to v2...")
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	fmt.Println("Upgrading bucket", string(logBucket))
	lb := tx.Bucket(logBucket)
	c := lb.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		var delta graph.Delta
		err := json.Unmarshal(v, &delta)
		if err != nil {
			return err
		}
		newd := deltaToProto(delta)
		data, err := newd.Marshal()
		if err != nil {
			return err
		}
		lb.Put(k, data)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	tx, err = db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	fmt.Println("Upgrading bucket", string(nodeBucket))
	nb := tx.Bucket(nodeBucket)
	c = nb.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		var vd proto.NodeData
		err := json.Unmarshal(v, &vd)
		if err != nil {
			return err
		}
		data, err := vd.Marshal()
		if err != nil {
			return err
		}
		nb.Put(k, data)
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	for _, bucket := range [4][]byte{spoBucket, ospBucket, posBucket, cpsBucket} {
		tx, err = db.Begin(true)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		fmt.Println("Upgrading bucket", string(bucket))
		b := tx.Bucket(bucket)
		cur := b.Cursor()
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			var h proto.HistoryEntry
			err := json.Unmarshal(v, &h)
			if err != nil {
				return err
			}
			data, err := h.Marshal()
			if err != nil {
				return err
			}
			b.Put(k, data)
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func upgrade2To3(db *bolt.DB) error {
	fmt.Println("Upgrading v2 to v3...")
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	fmt.Println("Upgrading bucket", string(logBucket))
	lb := tx.Bucket(logBucket)
	c := lb.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		var delta proto.LogDelta
		err := delta.Unmarshal(v)
		if err != nil {
			return err
		}
		delta.Quad.Upgrade()
		data, err := delta.Marshal()
		if err != nil {
			return err
		}
		lb.Put(k, data)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	tx, err = db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	fmt.Println("Upgrading bucket", string(nodeBucket))
	nb := tx.Bucket(nodeBucket)
	c = nb.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		var vd proto.NodeData
		err := vd.Unmarshal(v)
		if err != nil {
			return err
		}
		vd.Upgrade()
		data, err := vd.Marshal()
		if err != nil {
			return err
		}
		nb.Put(k, data)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}
