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

	"github.com/cayleygraph/cayley/clog"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
)

type AllIterator struct {
	uid    uint64
	tags   graph.Tagger
	qs     *QuadStore
	dir    quad.Direction
	val    graph.Value
	table  string
	cursor *sql.Rows
	result graph.Value
	err    error
}

func (it *AllIterator) makeCursor() {
	var cursor *sql.Rows
	var err error
	if it.cursor != nil {
		it.cursor.Close()
	}
	if it.table == "quads" {
		cursor, err = it.qs.db.Query(`SELECT
			subject_hash,
			predicate_hash,
			object_hash,
			label_hash
			FROM quads;`)
		if err != nil {
			clog.Errorf("Couldn't get cursor from SQL database: %v", err)
			cursor = nil
		}
	} else {
		if clog.V(4) {
			clog.Infof("sql: getting node query")
		}
		cursor, err = it.qs.db.Query(`SELECT hash FROM nodes;`)
		if err != nil {
			clog.Errorf("Couldn't get cursor from SQL database: %v", err)
			cursor = nil
		}
		if clog.V(4) {
			clog.Infof("sql: got node query")
		}
	}
	it.cursor = cursor
}

func NewAllIterator(qs *QuadStore, table string) *AllIterator {
	it := &AllIterator{
		uid:   iterator.NextUID(),
		qs:    qs,
		table: table,
	}
	return it
}

func (it *AllIterator) UID() uint64 {
	return it.uid
}

func (it *AllIterator) Reset() {
	it.err = nil
	it.Close()
}

func (it *AllIterator) Err() error {
	return it.err
}

func (it *AllIterator) Close() error {
	if it.cursor != nil {
		err := it.cursor.Close()
		if err != nil {
			return err
		}
		it.cursor = nil
	}
	return nil
}

func (it *AllIterator) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *AllIterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *AllIterator) Clone() graph.Iterator {
	var m *AllIterator
	m = NewAllIterator(it.qs, it.table)
	m.tags.CopyFrom(it)
	return m
}

func (it *AllIterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *AllIterator) Next() bool {
	graph.NextLogIn(it)
	if it.cursor == nil {
		it.makeCursor()
		if it.cursor == nil {
			return false
		}
	}
	if !it.cursor.Next() {
		if clog.V(4) {
			clog.Infof("sql: No next")
		}
		err := it.cursor.Err()
		if err != nil {
			clog.Errorf("Cursor error in SQL: %v", err)
			it.err = err
		}
		it.cursor.Close()
		return false
	}
	if it.table == "nodes" {
		var hash NodeHash
		err := it.cursor.Scan(&hash)
		if err != nil {
			clog.Errorf("Error nexting node iterator: %v", err)
			it.err = err
			return false
		}
		it.result = NodeHash(hash)
		return true
	}
	var q QuadHashes
	err := it.cursor.Scan(&q[0], &q[1], &q[2], &q[3])
	if err != nil {
		clog.Errorf("Error scanning sql iterator: %v", err)
		it.err = err
		return false
	}
	it.result = q
	return graph.NextLogOut(it, true)
}

func (it *AllIterator) Contains(v graph.Value) bool {
	graph.ContainsLogIn(it, v)
	it.result = v
	return graph.ContainsLogOut(it, v, true)
}

func (it *AllIterator) Size() (int64, bool) {
	return it.qs.Size(), true
}

func (it *AllIterator) Result() graph.Value {
	if it.result == nil {
		clog.Fatalf("result was nil %v", it)
	}
	return it.result
}

func (it *AllIterator) NextPath() bool {
	return false
}

func (it *AllIterator) Type() graph.Type {
	return graph.All
}

func (it *AllIterator) Sorted() bool                     { return false }
func (it *AllIterator) Optimize() (graph.Iterator, bool) { return it, false }

func (it *AllIterator) Describe() graph.Description {
	size, _ := it.Size()
	return graph.Description{
		UID:  it.UID(),
		Name: "sql/all",
		Type: it.Type(),
		Size: size,
	}
}

func (it *AllIterator) Stats() graph.IteratorStats {
	size, exact := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     9999,
		Size:         size,
		ExactSize:    exact,
	}
}
