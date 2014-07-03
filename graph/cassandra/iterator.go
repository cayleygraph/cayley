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

package cassandra

import (
	"fmt"
	"strings"

	"github.com/barakmich/glog"
	"github.com/gocql/gocql"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
)

type Iterator struct {
	iterator.Base
	ts     *TripleStore
	dir    graph.Direction
	isNode bool
	table  string
	iter   *gocql.Iter
	val    string
	size   int64
}

func NewIterator(ts *TripleStore, d graph.Direction, val graph.Value) graph.Iterator {
	it := &Iterator{}
	it.isNode = false
	it.ts = ts
	it.dir = d
	it.val = val.(string)
	it.table = fmt.Sprint("triples_by_", d)
	if it.dir == graph.Any {
		it.table = "triples_by_s"
		it.size = it.ts.Size()
	} else {
		err := it.ts.sess.Query(
			fmt.Sprint("SELECT COUNT(*) FROM ", it.table, " WHERE ", it.dir, " = ?"),
			it.val,
		).Scan(&it.size)
		if err != nil {
			glog.Errorln("Couldn't get size for iterator:", err)
			return iterator.NewNull()
		}
	}
	return it
}

func NewNodeIterator(ts *TripleStore) *Iterator {
	it := &Iterator{}
	it.ts = ts
	it.dir = graph.Any
	it.isNode = true
	return it
}

func (it *Iterator) closeIterator() {
	if it.iter != nil {
		err := it.iter.Close()
		if err != nil {
			glog.Errorln("Error closing iterator:", err)
		}
	}
}

func (it *Iterator) Reset() {
	it.closeIterator()
	it.iter = nil
}

func (it *Iterator) Close() {
	it.closeIterator()
}

func (it *Iterator) Clone() graph.Iterator {
	var newIt graph.Iterator
	if it.isNode {
		newIt = NewNodeIterator(it.ts)
	} else {
		newIt = NewIterator(it.ts, it.dir, it.val)
	}
	newIt.CopyTagsFrom(it)
	return newIt
}

func (it *Iterator) Check(v graph.Value) bool {
	graph.CheckLogIn(it, v)
	if it.dir == graph.Any || it.isNode {
		return graph.CheckLogOut(it, v, true)
	}
	triple := v.(*graph.Triple)
	if triple.Get(it.dir) == it.val {
		it.Last = &triple
		return graph.CheckLogOut(it, v, true)
	}
	return graph.CheckLogOut(it, v, false)
}

func (it *Iterator) prepareIterator() {
	it.iter = it.ts.sess.Query(
		fmt.Sprint(
			"SELECT subject, predicate, object, provenance FROM ",
			it.table,
			" WHERE ",
			it.dir,
			" = ?"),
		it.val,
	).Iter()
}

func (it *Iterator) Next() (graph.Value, bool) {
	triple := graph.Triple{}
	if it.iter == nil {
		it.prepareIterator()
	}
	ok := it.iter.Scan(
		&triple.Subject,
		&triple.Predicate,
		&triple.Object,
		&triple.Provenance,
	)
	if !ok {
		err := it.iter.Close()
		if err != nil {
			glog.Errorln("Iterator failed with", err)
		}
		return nil, false
	}
	it.Last = &triple
	return &triple, true
}

func (it *Iterator) Size() (int64, bool) {
	return it.size, true
}

func (it *Iterator) Optimize() (graph.Iterator, bool) { return it, false }
func (it *Iterator) Sorted() bool                     { return false }

func (it *Iterator) Type() string {
	if it.dir == graph.Any {
		return "all"
	}
	return "cassandra"
}

func (it *Iterator) DebugString(indent int) string {
	size, _ := it.Size()
	return fmt.Sprintf("%s(%s size:%d %s)", strings.Repeat(" ", indent), it.Type(), size, it.val)
}

func (it *Iterator) Stats() graph.IteratorStats {
	size, _ := it.Size()
	return graph.IteratorStats{
		CheckCost: 1,
		NextCost:  5,
		Size:      size,
	}
}
