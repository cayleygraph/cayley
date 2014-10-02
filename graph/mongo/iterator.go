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

	"github.com/barakmich/glog"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

type Iterator struct {
	uid        uint64
	tags       graph.Tagger
	qs         *QuadStore
	dir        quad.Direction
	iter       *mgo.Iter
	hash       string
	name       string
	size       int64
	isAll      bool
	constraint bson.M
	collection string
	result     graph.Value
}

func NewIterator(qs *QuadStore, collection string, d quad.Direction, val graph.Value) *Iterator {
	name := qs.NameOf(val)

	constraint := bson.M{d.String(): name}

	size, err := qs.db.C(collection).Find(constraint).Count()
	if err != nil {
		// FIXME(kortschak) This should be passed back rather than just logging.
		glog.Errorln("Trouble getting size for iterator! ", err)
		return nil
	}

	return &Iterator{
		uid:        iterator.NextUID(),
		name:       name,
		constraint: constraint,
		collection: collection,
		qs:         qs,
		dir:        d,
		iter:       qs.db.C(collection).Find(constraint).Iter(),
		size:       int64(size),
		hash:       val.(string),
		isAll:      false,
	}
}

func NewAllIterator(qs *QuadStore, collection string) *Iterator {
	size, err := qs.db.C(collection).Count()
	if err != nil {
		// FIXME(kortschak) This should be passed back rather than just logging.
		glog.Errorln("Trouble getting size for iterator! ", err)
		return nil
	}

	return &Iterator{
		uid:        iterator.NextUID(),
		qs:         qs,
		dir:        quad.Any,
		constraint: nil,
		collection: collection,
		iter:       qs.db.C(collection).Find(nil).Iter(),
		size:       int64(size),
		hash:       "",
		isAll:      true,
	}
}

func (it *Iterator) UID() uint64 {
	return it.uid
}

func (it *Iterator) Reset() {
	it.iter.Close()
	it.iter = it.qs.db.C(it.collection).Find(it.constraint).Iter()

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
		m = NewAllIterator(it.qs, it.collection)
	} else {
		m = NewIterator(it.qs, it.collection, it.dir, it.hash)
	}
	m.tags.CopyFrom(it)
	return m
}

func (it *Iterator) Next() bool {
	var result struct {
		ID      string  `bson:"_id"`
		Added   []int64 `bson:"Added"`
		Deleted []int64 `bson:"Deleted"`
	}
	found := it.iter.Next(&result)
	if !found {
		err := it.iter.Err()
		if err != nil {
			glog.Errorln("Error Nexting Iterator: ", err)
		}
		return false
	}
	if it.collection == "quads" && len(result.Added) <= len(result.Deleted) {
		return it.Next()
	}
	it.result = result.ID
	return true
}

func (it *Iterator) ResultTree() *graph.ResultTree {
	return graph.NewResultTree(it.Result())
}

func (it *Iterator) Result() graph.Value {
	return it.result
}

func (it *Iterator) NextPath() bool {
	return false
}

// No subiterators.
func (it *Iterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *Iterator) Contains(v graph.Value) bool {
	graph.ContainsLogIn(it, v)
	if it.isAll {
		it.result = v
		return graph.ContainsLogOut(it, v, true)
	}
	var offset int
	switch it.dir {
	case quad.Subject:
		offset = 0
	case quad.Predicate:
		offset = (hashSize * 2)
	case quad.Object:
		offset = (hashSize * 2) * 2
	case quad.Label:
		offset = (hashSize * 2) * 3
	}
	val := v.(string)[offset : hashSize*2+offset]
	if val == it.hash {
		it.result = v
		return graph.ContainsLogOut(it, v, true)
	}
	return graph.ContainsLogOut(it, v, false)
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

func (it *Iterator) Describe() graph.Description {
	size, _ := it.Size()
	return graph.Description{
		UID:  it.UID(),
		Name: fmt.Sprintf("%s/%s", it.name, it.hash),
		Type: it.Type(),
		Size: size,
	}
}

func (it *Iterator) Stats() graph.IteratorStats {
	size, _ := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     5,
		Size:         size,
	}
}
