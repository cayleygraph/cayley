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

	"github.com/barakmich/glog"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

var sqlType graph.Type

func init() {
	sqlType = graph.RegisterIterator("sql")
}

type SQLIterator struct {
	uid    uint64
	qs     *QuadStore
	cursor *sql.Rows
	err    error

	sql sqlIterator

	result      map[string]string
	resultIndex int
	resultList  [][]string
	resultNext  [][]string
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
	return sqlType
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
	size, _ := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     5,
		Size:         size,
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
			glog.Errorf("Couldn't make query: %v", err)
			it.err = err
			return false
		}
		it.cols, err = it.cursor.Columns()
		if err != nil {
			glog.Errorf("Couldn't get columns")
			it.err = err
			it.cursor.Close()
			return false
		}
		// iterate the first one
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
			glog.V(4).Infoln("sql: No next")
			err := it.cursor.Err()
			if err != nil {
				glog.Errorf("Cursor error in SQL: %v", err)
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
		return graph.NextLogOut(it, nil, false)
	}
	it.buildResult(0)
	return graph.NextLogOut(it, it.Result(), true)
}

func (it *SQLIterator) Contains(v graph.Value) bool {
	var err error
	if ok, res := it.sql.quickContains(v); ok {
		return res
	}
	err = it.makeCursor(false, v)
	if err != nil {
		glog.Errorf("Couldn't make query: %v", err)
		it.err = err
		if it.cursor != nil {
			it.cursor.Close()
		}
		return false
	}
	it.cols, err = it.cursor.Columns()
	if err != nil {
		glog.Errorf("Couldn't get columns")
		it.err = err
		it.cursor.Close()
		return false
	}
	it.resultList = nil
	for {
		if !it.cursor.Next() {
			glog.V(4).Infoln("sql: No next")
			err := it.cursor.Err()
			if err != nil {
				glog.Errorf("Cursor error in SQL: %v", err)
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

func scan(cursor *sql.Rows, nCols int) ([]string, error) {
	pointers := make([]interface{}, nCols)
	container := make([]string, nCols)
	for i, _ := range pointers {
		pointers[i] = &container[i]
	}
	err := cursor.Scan(pointers...)
	if err != nil {
		glog.Errorf("Error scanning iterator: %v", err)
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
	var values []string
	q, values = it.sql.buildSQL(next, value)
	q = convertToPostgres(q, values)
	ivalues := make([]interface{}, 0, len(values))
	for _, v := range values {
		ivalues = append(ivalues, v)
	}
	cursor, err := it.qs.db.Query(q, ivalues...)
	if err != nil {
		glog.Errorf("Couldn't get cursor from SQL database: %v", err)
		cursor = nil
		return err
	}
	it.cursor = cursor
	return nil
}

func convertToPostgres(query string, values []string) string {
	for i := 1; i <= len(values); i++ {
		query = strings.Replace(query, "?", fmt.Sprintf("$%d", i), 1)
	}
	return query
}

func NewSQLLinkIterator(qs *QuadStore, d quad.Direction, val string) *SQLIterator {
	l := &SQLIterator{
		uid: iterator.NextUID(),
		qs:  qs,
		sql: &SQLLinkIterator{
			constraints: []constraint{
				constraint{
					dir:  d,
					vals: []string{val},
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
