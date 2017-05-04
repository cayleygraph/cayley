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
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/proto"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/quad/pquads"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const latestDataVersion = 2
const nilDataVersion = 1

type upgradeFunc func(*leveldb.DB) error

var migrateFunctions = []upgradeFunc{
	nil,
	upgrade1To2,
}

func upgradeLevelDB(path string, opts graph.Options) error {
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

	type v1IndexEntry struct {
		Subject   string  `json:"subject"`
		Predicate string  `json:"predicate"`
		Object    string  `json:"object"`
		Label     string  `json:"label,omitempty"`
		History   []int64 `json:"History"`
	}

	type v1ValueData struct {
		Name string `json:"Name"`
		Size int64  `json:"Size"`
	}

	var (
		spoPref = []byte{spo[0].Prefix(), spo[1].Prefix()}
		ospPref = []byte{osp[0].Prefix(), osp[1].Prefix()}
		posPref = []byte{pos[0].Prefix(), pos[1].Prefix()}
		cpsPref = []byte{cps[0].Prefix(), cps[1].Prefix()}
	)

	{
		fmt.Println("Upgrading bucket z")
		it := db.NewIterator(&util.Range{Start: []byte{'z'}, Limit: []byte{'z' + 1}}, nil)
		for it.Next() {
			k, v := it.Key(), it.Value()
			var val v1ValueData
			if err := json.Unmarshal(v, &val); err != nil {
				return err
			}
			node := proto.NodeData{
				Size:  val.Size,
				Value: pquads.MakeValue(quad.Raw(val.Name)),
			}
			nv, err := node.Marshal()
			if err != nil {
				return err
			}
			if err = db.Put(k, nv, nil); err != nil {
				return err
			}
		}
		it.Release()
	}

	for _, pref := range [4][]byte{spoPref, ospPref, posPref, cpsPref} {
		fmt.Println("Upgrading bucket", string(pref))
		end := []byte{pref[0], pref[1] + 1}
		it := db.NewIterator(&util.Range{Start: pref, Limit: end}, nil)
		for it.Next() {
			k, v := it.Key(), it.Value()
			var entry v1IndexEntry
			if err := json.Unmarshal(v, &entry); err != nil {
				return err
			}
			var h proto.HistoryEntry
			h.History = make([]uint64, len(entry.History))
			for i, id := range entry.History {
				h.History[i] = uint64(id)
			}
			nv, err := h.Marshal()
			if err != nil {
				return err
			}
			if err = db.Put(k, nv, nil); err != nil {
				return err
			}
		}
		it.Release()
	}

	{
		fmt.Println("Upgrading bucket d")
		it := db.NewIterator(&util.Range{Start: []byte{'d'}, Limit: []byte{'d' + 1}}, nil)
		for it.Next() {
			k, v := it.Key(), it.Value()
			id, err := strconv.ParseInt(string(k[1:]), 16, 64)
			if err != nil {
				return err
			}
			nk := createDeltaKeyFor(id)
			var val graph.Delta
			if err := json.Unmarshal(v, &val); err != nil {
				return err
			}
			p := deltaToProto(val)
			nv, err := p.Marshal()
			if err != nil {
				return err
			}
			b := &leveldb.Batch{}
			b.Put(nk, nv)
			b.Delete(k)
			if err = db.Write(b, nil); err != nil {
				return err
			}
		}
		it.Release()
	}

	return nil
}
