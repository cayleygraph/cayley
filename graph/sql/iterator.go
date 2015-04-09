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

package sql

import (
	"database/sql"
	"fmt"

	"github.com/barakmich/glog"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

type Iterator struct {
	uid    uint64
	tags   graph.Tagger
	qs     *QuadStore
	dir    quad.Direction
	val    graph.Value
	size   int64
	isAll  bool
	table  string
	cursor *sql.Rows
	result graph.Value
	err    error
}

func (it *Iterator) makeCursor() {
	var cursor *sql.Rows
	var err error
	if it.cursor != nil {
		it.cursor.Close()
	}
	if it.isAll {
		if it.table == "quads" {
			cursor, err = it.qs.db.Query(`SELECT subject, predicate, object, label FROM quads;`)
			if err != nil {
				glog.Errorln("Couldn't get cursor from SQL database: %v", err)
				cursor = nil
			}
		} else {
			glog.V(4).Infoln("sql: getting node query")
			cursor, err = it.qs.db.Query(`SELECT node FROM
			(
				SELECT subject FROM quads
				UNION
				SELECT predicate FROM quads
				UNION
				SELECT object FROM quads
				UNION
				SELECT label FROM quads
			) AS DistinctNodes (node) WHERE node IS NOT NULL;`)
			if err != nil {
				glog.Errorln("Couldn't get cursor from SQL database: %v", err)
				cursor = nil
			}
			glog.V(4).Infoln("sql: got node query")
		}
	} else {
		cursor, err = it.qs.db.Query(
			fmt.Sprintf("SELECT subject, predicate, object, label FROM quads WHERE %s = $1;", it.dir.String()), it.val.(string))
		if err != nil {
			glog.Errorln("Couldn't get cursor from SQL database: %v", err)
			cursor = nil
		}
	}
	it.cursor = cursor
}

func NewIterator(qs *QuadStore, d quad.Direction, val graph.Value) *Iterator {
	it := &Iterator{
		uid:   iterator.NextUID(),
		qs:    qs,
		dir:   d,
		size:  -1,
		val:   val,
		table: "quads",
		isAll: false,
	}
	return it
}

func NewAllIterator(qs *QuadStore, table string) *Iterator {
	var size int64
	it := &Iterator{
		uid:   iterator.NextUID(),
		qs:    qs,
		dir:   quad.Any,
		size:  size,
		table: table,
		isAll: true,
	}
	return it
}

func (it *Iterator) UID() uint64 {
	return it.uid
}

func (it *Iterator) Reset() {
	it.err = nil
	it.Close()
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Close() error {
	if it.cursor != nil {
		err := it.cursor.Close()
		if err != nil {
			return err
		}
		it.cursor = nil
	}
	return nil
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
		m = NewAllIterator(it.qs, it.table)
	} else {
		m = NewIterator(it.qs, it.dir, it.val)
	}
	m.tags.CopyFrom(it)
	return m
}

func (it *Iterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *Iterator) Next() bool {
	graph.NextLogIn(it)
	if it.cursor == nil {
		it.makeCursor()
	}
	if !it.cursor.Next() {
		glog.V(4).Infoln("sql: No next")
		err := it.cursor.Err()
		if err != nil {
			glog.Errorf("Cursor error in SQL: %v", err)
			it.err = err
		}
		it.cursor.Close()
		return false
	}
	if it.table == "nodes" {
		var node string
		err := it.cursor.Scan(&node)
		if err != nil {
			glog.Errorf("Error nexting node iterator: %v", err)
			it.err = err
			return false
		}
		it.result = node
		return true
	}
	var q quad.Quad
	err := it.cursor.Scan(&q.Subject, &q.Predicate, &q.Object, &q.Label)
	if err != nil {
		glog.Errorf("Error scanning sql iterator: %v", err)
		it.err = err
		return false
	}
	it.result = q
	return graph.NextLogOut(it, it.result, true)
}

func (it *Iterator) Contains(v graph.Value) bool {
	graph.ContainsLogIn(it, v)
	if it.isAll {
		return graph.ContainsLogOut(it, v, true)
	}
	q := v.(quad.Quad)
	if q.Get(it.dir) == it.val.(string) {
		return graph.ContainsLogOut(it, v, true)
	}
	return graph.ContainsLogOut(it, v, false)
}

func (it *Iterator) Size() (int64, bool) {
	if it.size != -1 {
		return it.size, true
	}
	it.size = it.qs.sizeForIterator(it.isAll, it.dir, it.val.(string))
	return it.size, true
}

func (it *Iterator) Result() graph.Value {
	return it.result
}

func (it *Iterator) NextPath() bool {
	return false
}

var sqlType graph.Type

func init() {
	sqlType = graph.RegisterIterator("sql")
}

func Type() graph.Type { return sqlType }

func (it *Iterator) Type() graph.Type {
	if it.isAll {
		return graph.All
	}
	return sqlType
}

func (it *Iterator) Sorted() bool                     { return true }
func (it *Iterator) Optimize() (graph.Iterator, bool) { return it, false }

func (it *Iterator) Describe() graph.Description {
	size, _ := it.Size()
	return graph.Description{
		UID:  it.UID(),
		Name: fmt.Sprintf("%s/%s", it.val, it.dir),
		Type: it.Type(),
		Size: size,
	}
}

func (it *Iterator) Stats() graph.IteratorStats {
	size, _ := it.Size()
	if it.table == "nodes" {
		return graph.IteratorStats{
			ContainsCost: 1,
			NextCost:     9999,
			Size:         size,
		}
	}
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     5,
		Size:         size,
	}
}
