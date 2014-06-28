// Copyright 2014 The Cayley Authors. All rights reserved.
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

package mongo

import (
	"fmt"
	"strings"

	"github.com/barakmich/glog"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"

	"github.com/google/cayley/graph"
)

type MongoIterator struct {
	graph.BaseIterator
	ts         *MongoTripleStore
	dir        string
	iter       *mgo.Iter
	hash       string
	name       string
	size       int64
	isAll      bool
	constraint bson.M
	collection string
}

func NewMongoIterator(ts *MongoTripleStore, collection string, dir string, val graph.TSVal) *MongoIterator {
	var m MongoIterator
	graph.BaseIteratorInit(&m.BaseIterator)

	m.name = ts.GetNameFor(val)
	m.collection = collection
	switch dir {

	case "s":
		m.constraint = bson.M{"Sub": m.name}
	case "p":
		m.constraint = bson.M{"Pred": m.name}
	case "o":
		m.constraint = bson.M{"Obj": m.name}
	case "c":
		m.constraint = bson.M{"Provenance": m.name}
	}

	m.ts = ts
	m.dir = dir
	m.iter = ts.db.C(collection).Find(m.constraint).Iter()
	size, err := ts.db.C(collection).Find(m.constraint).Count()
	if err != nil {
		glog.Errorln("Trouble getting size for iterator! ", err)
		return nil
	}
	m.size = int64(size)
	m.hash = val.(string)
	m.isAll = false
	return &m
}

func NewMongoAllIterator(ts *MongoTripleStore, collection string) *MongoIterator {
	var m MongoIterator
	m.ts = ts
	m.dir = "all"
	m.constraint = nil
	m.collection = collection
	m.iter = ts.db.C(collection).Find(nil).Iter()
	size, err := ts.db.C(collection).Count()
	if err != nil {
		glog.Errorln("Trouble getting size for iterator! ", err)
		return nil
	}
	m.size = int64(size)
	m.hash = ""
	m.isAll = true
	return &m
}

func (m *MongoIterator) Reset() {
	m.iter.Close()
	m.iter = m.ts.db.C(m.collection).Find(m.constraint).Iter()

}

func (m *MongoIterator) Close() {
	m.iter.Close()
}

func (m *MongoIterator) Clone() graph.Iterator {
	var newM graph.Iterator
	if m.isAll {
		newM = NewMongoAllIterator(m.ts, m.collection)
	} else {
		newM = NewMongoIterator(m.ts, m.collection, m.dir, m.hash)
	}
	newM.CopyTagsFrom(m)
	return newM
}

func (m *MongoIterator) Next() (graph.TSVal, bool) {
	var result struct {
		Id string "_id"
		//Sub string "Sub"
		//Pred string "Pred"
		//Obj string "Obj"
	}
	found := m.iter.Next(&result)
	if !found {
		err := m.iter.Err()
		if err != nil {
			glog.Errorln("Error Nexting MongoIterator: ", err)
		}
		return nil, false
	}
	m.Last = result.Id
	return result.Id, true
}

func (m *MongoIterator) Check(v graph.TSVal) bool {
	graph.CheckLogIn(m, v)
	if m.isAll {
		m.Last = v
		return graph.CheckLogOut(m, v, true)
	}
	var offset int
	switch m.dir {
	case "s":
		offset = 0
	case "p":
		offset = (m.ts.hasher.Size() * 2)
	case "o":
		offset = (m.ts.hasher.Size() * 2) * 2
	case "c":
		offset = (m.ts.hasher.Size() * 2) * 3
	}
	val := v.(string)[offset : m.ts.hasher.Size()*2+offset]
	if val == m.hash {
		m.Last = v
		return graph.CheckLogOut(m, v, true)
	}
	return graph.CheckLogOut(m, v, false)
}

func (m *MongoIterator) Size() (int64, bool) {
	return m.size, true
}

func (m *MongoIterator) Type() string {
	if m.isAll {
		return "all"
	}
	return "mongo"
}
func (m *MongoIterator) Sorted() bool                     { return true }
func (m *MongoIterator) Optimize() (graph.Iterator, bool) { return m, false }

func (m *MongoIterator) DebugString(indent int) string {
	size, _ := m.Size()
	return fmt.Sprintf("%s(%s size:%d %s %s)", strings.Repeat(" ", indent), m.Type(), size, m.hash, m.name)
}

func (m *MongoIterator) GetStats() *graph.IteratorStats {
	size, _ := m.Size()
	return &graph.IteratorStats{
		CheckCost: 1,
		NextCost:  5,
		Size:      size,
	}
}
