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
	"fmt"

	"github.com/barakmich/glog"
	"github.com/boltdb/bolt"
	"github.com/google/cayley/graph"
)

const latestDataVersion = 1
const nilDataVersion = 1

type upgradeFunc func(*bolt.DB) error

var migrateFunctions = []upgradeFunc{
	nil,
	upgrade1To2,
}

func upgradeBolt(path string, opts graph.Options) error {
	db, err := bolt.Open(path, 0600, nil)
	defer db.Close()
	if err != nil {
		glog.Errorln("Error, couldn't open! ", err)
		return err
	}
	var version int64
	err = db.View(func(tx *bolt.Tx) error {
		version, err = getInt64ForMetaKey(tx, "version", nilDataVersion)
		return err
	})
	if err != nil {
		glog.Errorln("error:", err)
		return err
	}

	if version == latestDataVersion {
		fmt.Printf("Already at latest version: %d\n", latestDataVersion)
		return nil
	}

	if version > latestDataVersion {
		err := fmt.Errorf("Unknown data version: %d -- upgrade this tool", version)
		glog.Errorln("error:", err)
		return err
	}

	for i := version; i < latestDataVersion; i++ {
		err := migrateFunctions[i](db)
		if err != nil {
			return err
		}
	}

	return nil
}

func upgrade1To2(db *bolt.DB) error {
	fmt.Println("Upgrading v1 to v2...")
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}
