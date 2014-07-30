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
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
)

type Iterator struct {
	iterator.Base
	uid        uint64
	tags       graph.Tagger
	ts         *TripleStore
	dir        graph.Direction
	iter       *mgo.Iter
	hash       string
	name       string
	size       int64
	isAll      bool
	constraint bson.M
	collection string
	result     graph.Value
}

func NewIterator(ts *TripleStore, collection string, d graph.Direction, val graph.Value) *Iterator {
	name := ts.NameOf(val)

	var constraint bson.M
	switch d {
	case graph.Subject:
		constraint = bson.M{"Subject": name}
	case graph.Predicate:
		constraint = bson.M{"Predicate": name}
	case graph.Object:
		constraint = bson.M{"Object": name}
	case graph.Provenance:
		constraint = bson.M{"Provenance": name}
	}

	size, err := ts.db.C(collection).Find(constraint).Count()
	if err != nil {
		// FIXME(kortschak) This should be passed back rather than just logging.
		glog.Errorln("Trouble getting size for iterator! ", err)
		return nil
	}

	m := Iterator{
		uid:        iterator.NextUID(),
		name:       name,
		constraint: constraint,
		collection: collection,
		ts:         ts,
		dir:        d,
		iter:       ts.db.C(collection).Find(constraint).Iter(),
		size:       int64(size),
		hash:       val.(string),
		isAll:      false,
	}
	iterator.BaseInit(&m.Base)

	return &m
}

func NewAllIterator(ts *TripleStore, collection string) *Iterator {
	size, err := ts.db.C(collection).Count()
	if err != nil {
		// FIXME(kortschak) This should be passed back rather than just logging.
		glog.Errorln("Trouble getting size for iterator! ", err)
		return nil
	}

	m := Iterator{
		uid:        iterator.NextUID(),
		ts:         ts,
		dir:        graph.Any,
		constraint: nil,
		collection: collection,
		iter:       ts.db.C(collection).Find(nil).Iter(),
		size:       int64(size),
		hash:       "",
		isAll:      true,
	}
	// FIXME(kortschak) Was there supposed to be a BaseInit call here?

	return &m
}

func (it *Iterator) UID() uint64 {
	return it.uid
}

func (it *Iterator) Reset() {
	it.iter.Close()
	it.iter = it.ts.db.C(it.collection).Find(it.constraint).Iter()

}

func (it *Iterator) Close() {
	it.iter.Close()
}

func (it *Iterator) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *Iterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *Iterator) Clone() graph.Iterator {
	var m *Iterator
	if it.isAll {
		m = NewAllIterator(it.ts, it.collection)
	} else {
		m = NewIterator(it.ts, it.collection, it.dir, it.hash)
	}
	m.tags.CopyFrom(it)
	return m
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
	it.result = result.Id
	return result.Id, true
}

func (it *Iterator) ResultTree() *graph.ResultTree {
	return graph.NewResultTree(it.Result())
}

func (it *Iterator) Result() graph.Value {
	return it.result
}

// No subiterators.
func (it *Iterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *Iterator) Check(v graph.Value) bool {
	graph.CheckLogIn(it, v)
	if it.isAll {
		it.result = v
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
		it.result = v
		return graph.CheckLogOut(it, v, true)
	}
	return graph.CheckLogOut(it, v, false)
}

func (it *Iterator) Size() (int64, bool) {
	return it.size, true
}

var mongoType graph.Type

func init() {
	mongoType = graph.RegisterIterator("mongo")
}

func Type() graph.Type { return mongoType }

func (it *Iterator) Type() graph.Type {
	if it.isAll {
		return graph.All
	}
	return mongoType
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
