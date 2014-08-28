// Copyright 2014 The Cayley Authors. All righqs reserved.
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
	"github.com/google/cayley/quad"
)

var cassandraType graph.Type

func init() {
	cassandraType = graph.RegisterIterator("cassandra")
}

type Iterator struct {
	uid     uint64
	qs      *QuadStore
	dir     quad.Direction
	isNode  bool
	table   string
	iter    *gocql.Iter
	val     string
	size    int64
	hasSize bool
	tags    graph.Tagger
	result  graph.Value
}

func NewIterator(qs *QuadStore, d quad.Direction, val graph.Value) graph.Iterator {
	it := &Iterator{
		uid: iterator.NextUID(),
		qs:  qs,
		dir: d,
		val: val.(string),
	}
	it.isNode = false
	it.table = fmt.Sprint("quads_by_", string(d.Prefix()))
	if it.dir == quad.Any {
		it.table = "quads_by_s"
	}
	return it
}

func NewNodeIterator(qs *QuadStore) *Iterator {
	it := &Iterator{
		qs:  qs,
		dir: quad.Any,
	}
	it.isNode = true
	return it
}

func (it *Iterator) UID() uint64 {
	return it.uid
}

func (it *Iterator) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *Iterator) Result() graph.Value {
	return it.result
}

func (it *Iterator) ResultTree() *graph.ResultTree {
	return graph.NewResultTree(it.Result())
}

// No subiterators.
func (it *Iterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *Iterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
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
		newIt = NewNodeIterator(it.qs)
	} else {
		newIt = NewIterator(it.qs, it.dir, it.val)
	}
	newIt.Tagger().CopyFrom(it)
	return newIt
}

func (it *Iterator) NextPath() bool {
	return false
}

func (it *Iterator) Contains(v graph.Value) bool {
	graph.ContainsLogIn(it, v)
	if it.dir == quad.Any || it.isNode {
		return graph.ContainsLogOut(it, v, true)
	}
	triple := v.(quad.Quad)
	if triple.Get(it.dir) == it.val {
		it.result = &triple
		return graph.ContainsLogOut(it, v, true)
	}
	return graph.ContainsLogOut(it, v, false)
}

func (it *Iterator) prepareIterator() {
	if it.isNode {
		it.iter = it.qs.sess.Query("SELECT node FROM nodes").Iter()
	} else {
		it.iter = it.qs.sess.Query(
			fmt.Sprint(
				"SELECT subject, predicate, object, label, created, deleted FROM ",
				it.table,
				" WHERE ",
				it.dir,
				" = ?"),
			it.val,
		).Iter()
	}
}

func (it *Iterator) Next() bool {
	if it.iter == nil {
		it.prepareIterator()
	}
	if it.isNode {
		return it.nodeNext()
	}
	return it.tripleNext()
}

func (it *Iterator) nodeNext() bool {
	var node string
	ok := it.iter.Scan(&node)
	if !ok {
		err := it.iter.Close()
		if err != nil {
			glog.Errorln("Iterator failed with", err)
		}
		return false
	}
	it.result = node
	return true
}

func (it *Iterator) tripleNext() bool {
	q := quad.Quad{}
	var created []int64
	var deleted []int64

	ok := it.iter.Scan(
		&q.Subject,
		&q.Predicate,
		&q.Object,
		&q.Label,
		&created,
		&deleted,
	)
	if !ok {
		err := it.iter.Close()
		if err != nil {
			glog.Errorln("Iterator failed with", err)
		}
		return false
	}
	if len(deleted) != 0 {
		if len(created) == 0 || created[len(created)-1] < deleted[len(deleted)-1] {
			return it.Next()
		}
	}
	it.result = q
	return true
}

func (it *Iterator) Size() (int64, bool) {
	if it.hasSize {
		return it.size, true
	}
	if it.dir == quad.Any {
		return it.qs.Size(), true
	}
	err := it.qs.sess.Query(
		fmt.Sprint("SELECT ", it.dir, "_count FROM nodes WHERE node = ?"),
		it.val,
	).Scan(&it.size)
	if err != nil {
		glog.Errorln("Couldn't get size for iterator:", err)
		return int64(1 << 62), false
	}
	it.hasSize = true
	return it.size, true
}

func (it *Iterator) Optimize() (graph.Iterator, bool) { return it, false }
func (it *Iterator) Sorted() bool                     { return false }
func (it *Iterator) CanNext() bool                    { return true }

func (it *Iterator) Type() graph.Type {
	if it.dir == quad.Any {
		return graph.All
	}
	return cassandraType
}

func (it *Iterator) DebugString(indent int) string {
	size, _ := it.Size()
	return fmt.Sprintf("%s(%s size:%d %s)", strings.Repeat(" ", indent), it.Type(), size, it.val)
}

func (it *Iterator) Stats() graph.IteratorStats {
	size, _ := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     5,
		Size:         size,
	}
}
