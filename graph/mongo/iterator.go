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
	"github.com/google/cayley/graph/iterator"
)

type Iterator struct {
	iterator.Base
	ts         *TripleStore
	dir        graph.Direction
	iter       *mgo.Iter
	hash       string
	name       string
	size       int64
	isAll      bool
	constraint bson.M
	collection string
}

func NewIterator(ts *TripleStore, collection string, d graph.Direction, val graph.Value) *Iterator {
	var m Iterator
	iterator.BaseInit(&m.Base)

	m.name = ts.NameOf(val)
	m.collection = collection
	switch d {
	case graph.Subject:
		m.constraint = bson.M{"Subject": m.name}
	case graph.Predicate:
		m.constraint = bson.M{"Predicate": m.name}
	case graph.Object:
		m.constraint = bson.M{"Object": m.name}
	case graph.Provenance:
		m.constraint = bson.M{"Provenance": m.name}
	}

	m.ts = ts
	m.dir = d
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

func NewAllIterator(ts *TripleStore, collection string) *Iterator {
	var m Iterator
	m.ts = ts
	m.dir = graph.Any
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

func (it *Iterator) Reset() {
	it.iter.Close()
	it.iter = it.ts.db.C(it.collection).Find(it.constraint).Iter()

}

func (it *Iterator) Close() {
	it.iter.Close()
}

func (it *Iterator) Clone() graph.Iterator {
	var newM graph.Iterator
	if it.isAll {
		newM = NewAllIterator(it.ts, it.collection)
	} else {
		newM = NewIterator(it.ts, it.collection, it.dir, it.hash)
	}
	newM.CopyTagsFrom(it)
	return newM
}

func (it *Iterator) Next() (graph.Value, bool) {
	var result struct {
		Id string "_id"
		//Sub string "Sub"
		//Pred string "Pred"
		//Obj string "Obj"
	}
	found := it.iter.Next(&result)
	if !found {
		err := it.iter.Err()
		if err != nil {
			glog.Errorln("Error Nexting Iterator: ", err)
		}
		return nil, false
	}
	it.Last = result.Id
	return result.Id, true
}

func (it *Iterator) Check(v graph.Value) bool {
	graph.CheckLogIn(it, v)
	if it.isAll {
		it.Last = v
		return graph.CheckLogOut(it, v, true)
	}
	var offset int
	switch it.dir {
	case graph.Subject:
		offset = 0
	case graph.Predicate:
		offset = (it.ts.hasher.Size() * 2)
	case graph.Object:
		offset = (it.ts.hasher.Size() * 2) * 2
	case graph.Provenance:
		offset = (it.ts.hasher.Size() * 2) * 3
	}
	val := v.(string)[offset : it.ts.hasher.Size()*2+offset]
	if val == it.hash {
		it.Last = v
		return graph.CheckLogOut(it, v, true)
	}
	return graph.CheckLogOut(it, v, false)
}

func (it *Iterator) Size() (int64, bool) {
	return it.size, true
}

func (it *Iterator) Type() string {
	if it.isAll {
		return "all"
	}
	return "mongo"
}
func (it *Iterator) Sorted() bool                     { return true }
func (it *Iterator) Optimize() (graph.Iterator, bool) { return it, false }

func (it *Iterator) DebugString(indent int) string {
	size, _ := it.Size()
	return fmt.Sprintf("%s(%s size:%d %s %s)", strings.Repeat(" ", indent), it.Type(), size, it.hash, it.name)
}

func (it *Iterator) Stats() graph.IteratorStats {
	size, _ := it.Size()
	return graph.IteratorStats{
		CheckCost: 1,
		NextCost:  5,
		Size:      size,
	}
}
