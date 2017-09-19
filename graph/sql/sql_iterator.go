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

package sql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
)

type sqlArgs []interface{}

type tagDir struct {
	tag       string
	dir       quad.Direction
	table     string
	justLocal bool
}

func (t tagDir) String() string { return "" }
func (t tagDir) SQL(escape func(string)string) string {
	tag := escape(t.tag)
	if t.dir == quad.Any {
		if t.justLocal {
			return fmt.Sprintf("%s.__execd as %s", t.table, tag)
		}
		return fmt.Sprintf("%s.%s as %s", t.table, tag, tag)
	}
	return fmt.Sprintf("%s.%s_hash as %s", t.table, t.dir, tag)
}

type tableDef struct {
	table  string
	name   string
	values sqlArgs
}

type sqlIterator interface {
	sqlClone() sqlIterator

	buildSQL(fl *Registration, next bool, val graph.Value) (string, sqlArgs)
	getTables(fl *Registration) []tableDef
	getTags() []tagDir
	buildWhere() (string, sqlArgs)
	tableID() tagDir

	quickContains(graph.Value) (ok bool, result bool)
	buildResult(result []NodeHash, cols []string) map[string]graph.Value
	sameTopResult(target []NodeHash, test []NodeHash) bool

	Result() graph.Value
	Size(*QuadStore) (int64, bool)
	Describe() string
	Type() sqlQueryType
	Tagger() *graph.Tagger
}

type SQLIterator struct {
	uid    uint64
	qs     *QuadStore
	cursor *sql.Rows
	err    error

	sql sqlIterator

	result      map[string]graph.Value
	resultIndex int
	resultList  [][]NodeHash
	resultNext  [][]NodeHash
	cols        []string
}

func (it *SQLIterator) Clone() graph.Iterator {
	m := &SQLIterator{
		uid: iterator.NextUID(),
		qs:  it.qs,
		sql: it.sql.sqlClone(),
	}
	return m
}

func (it *SQLIterator) UID() uint64 {
	return it.uid
}

func (it *SQLIterator) Reset() {
	it.err = nil
	it.Close()
}

func (it *SQLIterator) Err() error {
	return it.err
}

func (it *SQLIterator) Close() error {
	if it.cursor != nil {
		err := it.cursor.Close()
		if err != nil {
			return err
		}
		it.cursor = nil
	}
	return nil
}

func (it *SQLIterator) Tagger() *graph.Tagger {
	return it.sql.Tagger()
}

func (it *SQLIterator) Result() graph.Value {
	return it.sql.Result()
}

func (it *SQLIterator) TagResults(dst map[string]graph.Value) {
	for tag, value := range it.result {
		if tag == "__execd" {
			for _, tag := range it.Tagger().Tags() {
				dst[tag] = value
			}
			continue
		}
		dst[tag] = value
	}

	for tag, value := range it.Tagger().Fixed() {
		dst[tag] = value
	}
}

func (it *SQLIterator) Type() graph.Type {
	return "sql"
}

func (it *SQLIterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *SQLIterator) Sorted() bool                     { return false }
func (it *SQLIterator) Optimize() (graph.Iterator, bool) { return it, false }

func (it *SQLIterator) Size() (int64, bool) {
	return it.sql.Size(it.qs)
}

func (it *SQLIterator) Describe() graph.Description {
	size, _ := it.Size()
	return graph.Description{
		UID:  it.UID(),
		Name: it.sql.Describe(),
		Type: it.Type(),
		Size: size,
	}
}

func (it *SQLIterator) Stats() graph.IteratorStats {
	size, exact := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     5,
		Size:         size,
		ExactSize:    exact,
	}
}

func (it *SQLIterator) NextPath() bool {
	it.resultIndex += 1
	if it.resultIndex >= len(it.resultList) {
		return false
	}
	it.buildResult(it.resultIndex)
	return true
}

func (it *SQLIterator) Next() bool {
	var err error
	graph.NextLogIn(it)
	if it.cursor == nil {
		err = it.makeCursor(true, nil)
		if err != nil {
			clog.Errorf("Couldn't make query: %v", err)
			it.err = err
			return false
		}
		it.cols, err = it.cursor.Columns()
		if err != nil {
			clog.Errorf("Couldn't get columns")
			it.err = err
			it.cursor.Close()
			return false
		}
		// iterate the first one
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
		s, err := scan(it.cursor, len(it.cols))
		if err != nil {
			it.err = err
			it.cursor.Close()
			return false
		}
		it.resultNext = append(it.resultNext, s)
	}
	if it.resultList != nil && it.resultNext == nil {
		// We're on something and there's no next
		return false
	}
	it.resultList = it.resultNext
	it.resultNext = nil
	it.resultIndex = 0
	for {
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
			break
		}
		s, err := scan(it.cursor, len(it.cols))
		if err != nil {
			it.err = err
			it.cursor.Close()
			return false
		}

		if it.sql.sameTopResult(it.resultList[0], s) {
			it.resultList = append(it.resultList, s)
		} else {
			it.resultNext = append(it.resultNext, s)
			break
		}
	}

	if len(it.resultList) == 0 {
		return graph.NextLogOut(it, false)
	}
	it.buildResult(0)
	return graph.NextLogOut(it, true)
}

func (it *SQLIterator) Contains(v graph.Value) bool {
	var err error
	if ok, res := it.sql.quickContains(v); ok {
		return res
	}
	err = it.makeCursor(false, v)
	if err != nil {
		clog.Errorf("Couldn't make query: %v", err)
		it.err = err
		if it.cursor != nil {
			it.cursor.Close()
		}
		return false
	}
	it.cols, err = it.cursor.Columns()
	if err != nil {
		clog.Errorf("Couldn't get columns")
		it.err = err
		it.cursor.Close()
		return false
	}
	it.resultList = nil
	for {
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
			break
		}
		s, err := scan(it.cursor, len(it.cols))
		if err != nil {
			it.err = err
			it.cursor.Close()
			return false
		}
		it.resultList = append(it.resultList, s)
	}
	it.cursor.Close()
	it.cursor = nil
	if len(it.resultList) != 0 {
		it.resultIndex = 0
		it.buildResult(0)
		return true
	}
	return false
}

func scan(cursor *sql.Rows, nCols int) ([]NodeHash, error) {
	pointers := make([]interface{}, nCols)
	container := make([]NodeHash, nCols)
	for i, _ := range pointers {
		pointers[i] = &container[i]
	}
	err := cursor.Scan(pointers...)
	if err != nil {
		clog.Errorf("Error scanning iterator: %v", err)
		return nil, err
	}
	return container, nil
}

func (it *SQLIterator) buildResult(i int) {
	it.result = it.sql.buildResult(it.resultList[i], it.cols)
}

func (it *SQLIterator) makeCursor(next bool, value graph.Value) error {
	if it.cursor != nil {
		it.cursor.Close()
	}
	var q string
	var values sqlArgs
	q, values = it.sql.buildSQL(&it.qs.flavor, next, value)
	if it.qs.flavor.Placeholder(1) != "?" {
		q = convertPlaceholders(q, values)
	}
	cursor, err := it.qs.db.Query(q, values...)
	if err != nil {
		clog.Errorf("Couldn't get cursor from SQL database: %v", err)
		cursor = nil
		return err
	}
	it.cursor = cursor
	return nil
}

func convertPlaceholders(query string, values sqlArgs) string {
	for i := 1; i <= len(values); i++ {
		query = strings.Replace(query, "?", fmt.Sprintf("$%d", i), 1)
	}
	return query
}

func NewSQLLinkIterator(qs *QuadStore, d quad.Direction, v quad.Value) *SQLIterator {
	return newSQLLinkIterator(qs, d, NodeHash(HashOf(v)))
}

func newSQLLinkIterator(qs *QuadStore, d quad.Direction, hash NodeHash) *SQLIterator {
	l := &SQLIterator{
		uid: iterator.NextUID(),
		qs:  qs,
		sql: &SQLLinkIterator{
			constraints: []constraint{
				{
					dir:    d,
					hashes: []NodeHash{hash},
				},
			},
			tableName: newTableName(),
			size:      0,
		},
	}
	return l
}

func NewSQLIterator(qs *QuadStore, sql sqlIterator) *SQLIterator {
	l := &SQLIterator{
		uid: iterator.NextUID(),
		qs:  qs,
		sql: sql,
	}
	return l
}
