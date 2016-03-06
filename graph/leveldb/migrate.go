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

package leveldb

import (
	//"encoding/json"
	"fmt"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const latestDataVersion = 2
const nilDataVersion = 1

type upgradeFunc func(*leveldb.DB) error

var migrateFunctions = []upgradeFunc{
	nil,
	upgrade1To2,
}

type v1IndexEntry struct {
	Subject   string `json:"subject"`
	Predicate string `json:"predicate"`
	Object    string `json:"object"`
	Label     string `json:"label,omitempty"`
	History   []int64
}

type v1ValueData struct {
	Name string
	Size int64
}

func upgradeLevelDB(path string, opts graph.Options) error {
	return nil
	db, err := leveldb.OpenFile(path, &opt.Options{})
	defer db.Close()

	if err != nil {
		clog.Errorf("Error, couldn't open! %v", err)
		return err
	}
	version, err := getVersion(db)
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
		setVersion(db, i+1, nil)
	}

	return nil
}

func upgrade1To2(db *leveldb.DB) error {
	fmt.Println("Upgrading v1 to v2...")
	return fmt.Errorf("not implemented yet")
}
