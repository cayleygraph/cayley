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
	"sync/atomic"

	"github.com/barakmich/glog"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

var sqlLinkType graph.Type
var sqlTableID uint64

func init() {
	sqlLinkType = graph.RegisterIterator("sqllink")
	sqlNodeType = graph.RegisterIterator("sqlnode")
	atomic.StoreUint64(&sqlTableID, 0)
}

func newTableName() string {
	id := atomic.AddUint64(&sqlTableID, 1)
	return fmt.Sprintf("t_%d", id)
}

type constraint struct {
	dir  quad.Direction
	vals []string
}

type tagDir struct {
	tag string
	dir quad.Direction

	// Not to be stored in the iterator directly
	table string
}

type sqlItDir struct {
	dir quad.Direction
	it  sqlIterator
}

type sqlIterator interface {
	sqlClone() sqlIterator
	getTables() []string
	getTags() []tagDir
	buildWhere() (string, []string)
	tableID() tagDir
	height() int
}

type SQLLinkIterator struct {
	uid    uint64
	qs     *QuadStore
	tagger graph.Tagger
	err    error
	next   bool

	cursor      *sql.Rows
	nodeIts     []sqlItDir
	constraints []constraint
	tableName   string
	size        int64

	result      map[string]string
	resultIndex int
	resultList  [][]string
	resultNext  [][]string
	cols        []string
	resultQuad  quad.Quad
}

func NewSQLLinkIterator(qs *QuadStore, d quad.Direction, val string) *SQLLinkIterator {
	l := &SQLLinkIterator{
		uid: iterator.NextUID(),
		qs:  qs,
		constraints: []constraint{
			constraint{
				dir:  d,
				vals: []string{val},
			},
		},
		tableName: newTableName(),
		size:      0,
	}
	return l
}

func (l *SQLLinkIterator) sqlClone() sqlIterator {
	return l.Clone().(*SQLLinkIterator)
}

func (l *SQLLinkIterator) Clone() graph.Iterator {
	m := &SQLLinkIterator{
		uid:       iterator.NextUID(),
		qs:        l.qs,
		tableName: l.tableName,
		size:      l.size,
	}
	for _, i := range l.nodeIts {
		m.nodeIts = append(m.nodeIts, sqlItDir{
			dir: i.dir,
			it:  i.it.sqlClone(),
		})
	}
	m.constraints = l.constraints[:]
	m.tagger.CopyFrom(l)
	return m
}

func (l *SQLLinkIterator) UID() uint64 {
	return l.uid
}

func (l *SQLLinkIterator) Reset() {
	l.err = nil
	l.Close()
}

func (l *SQLLinkIterator) Err() error {
	return l.err
}

func (l *SQLLinkIterator) Close() error {
	if l.cursor != nil {
		err := l.cursor.Close()
		if err != nil {
			return err
		}
		l.cursor = nil
	}
	return nil
}

func (l *SQLLinkIterator) Tagger() *graph.Tagger {
	return &l.tagger
}

func (l *SQLLinkIterator) Result() graph.Value {
	return l.resultQuad
}

func (l *SQLLinkIterator) TagResults(dst map[string]graph.Value) {
	for tag, value := range l.result {
		if tag == "__execd" {
			for _, tag := range l.tagger.Tags() {
				dst[tag] = value
			}
			continue
		}
		dst[tag] = value
	}

	for tag, value := range l.tagger.Fixed() {
		dst[tag] = value
	}
}

func (l *SQLLinkIterator) SubIterators() []graph.Iterator {
	// TODO(barakmich): SQL Subiterators shouldn't count? If it makes sense,
	// there's no reason not to expose them though.
	return nil
}

func (l *SQLLinkIterator) Sorted() bool                     { return false }
func (l *SQLLinkIterator) Optimize() (graph.Iterator, bool) { return l, false }

func (l *SQLLinkIterator) Size() (int64, bool) {
	if l.size != 0 {
		return l.size, true
	}
	if len(l.constraints) > 0 {
		l.size = l.qs.sizeForIterator(false, l.constraints[0].dir, l.constraints[0].vals[0])
	} else {
		return l.qs.Size(), false
	}
	return l.size, true
}

func (l *SQLLinkIterator) Describe() graph.Description {
	size, _ := l.Size()
	return graph.Description{
		UID:  l.UID(),
		Name: fmt.Sprintf("SQL_LINK_QUERY: %#v", l),
		Type: l.Type(),
		Size: size,
	}
}

func (l *SQLLinkIterator) Stats() graph.IteratorStats {
	size, _ := l.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     5,
		Size:         size,
	}
}

func (l *SQLLinkIterator) Type() graph.Type {
	return sqlLinkType
}

func (l *SQLLinkIterator) Contains(v graph.Value) bool {
	var err error
	//if it.preFilter(v) {
	//return false
	//}
	err = l.makeCursor(false, v)
	if err != nil {
		glog.Errorf("Couldn't make query: %v", err)
		l.err = err
		l.cursor.Close()
		return false
	}
	l.cols, err = l.cursor.Columns()
	if err != nil {
		glog.Errorf("Couldn't get columns")
		l.err = err
		l.cursor.Close()
		return false
	}
	l.resultList = nil
	for {
		if !l.cursor.Next() {
			glog.V(4).Infoln("sql: No next")
			err := l.cursor.Err()
			if err != nil {
				glog.Errorf("Cursor error in SQL: %v", err)
				l.err = err
			}
			l.cursor.Close()
			break
		}
		s, err := scan(l.cursor, len(l.cols))
		if err != nil {
			l.err = err
			l.cursor.Close()
			return false
		}
		l.resultList = append(l.resultList, s)
	}
	l.cursor.Close()
	l.cursor = nil
	if len(l.resultList) != 0 {
		l.resultIndex = 0
		l.buildResult(0)
		return true
	}
	return false
}

func (l *SQLLinkIterator) NextPath() bool {
	l.resultIndex += 1
	if l.resultIndex >= len(l.resultList) {
		return false
	}
	l.buildResult(l.resultIndex)
	return true
}

func (l *SQLLinkIterator) buildResult(i int) {
	container := l.resultList[i]
	var q quad.Quad
	q.Subject = container[0]
	q.Predicate = container[1]
	q.Object = container[2]
	q.Label = container[3]
	l.resultQuad = q
	l.result = make(map[string]string)
	for i, c := range l.cols[4:] {
		l.result[c] = container[i+4]
	}
}

func (l *SQLLinkIterator) getTables() []string {
	out := []string{l.tableName}
	//for _, i := range l.nodeIts {
	//out = append(out, i.it.getTables()...)
	//}
	return out
}

func (l *SQLLinkIterator) height() int {
	v := 0
	for _, i := range l.nodeIts {
		if i.it.height() > v {
			v = i.it.height()
		}
	}
	return v + 1
}

func (l *SQLLinkIterator) getTags() []tagDir {
	var out []tagDir
	for _, tag := range l.tagger.Tags() {
		out = append(out, tagDir{
			dir:   quad.Any,
			table: l.tableName,
			tag:   tag,
		})
	}
	//for _, i := range l.nodeIts {
	//out = append(out, i.it.getTags()...)
	//}
	return out
}

func (l *SQLLinkIterator) buildWhere() (string, []string) {
	var q []string
	var vals []string
	for _, c := range l.constraints {
		q = append(q, fmt.Sprintf("%s.%s = ?", l.tableName, c.dir))
		vals = append(vals, c.vals[0])
	}
	for _, i := range l.nodeIts {
		sni := i.it.(*SQLNodeIterator)
		sql, s := sni.buildSQL(true, nil)
		q = append(q, fmt.Sprintf("%s.%s in (%s)", l.tableName, i.dir, sql[:len(sql)-1]))
		vals = append(vals, s...)
		//q = append(q, fmt.Sprintf("%s.%s = %s.%s", l.tableName, i.dir, t.table, t.dir))
	}
	//for _, i := range l.nodeIts {
	//s, v := i.it.buildWhere()
	//q = append(q, s)
	//vals = append(vals, v...)
	//}
	query := strings.Join(q, " AND ")
	return query, vals
}

func (l *SQLLinkIterator) tableID() tagDir {
	return tagDir{
		dir:   quad.Any,
		table: l.tableName,
	}
}

func (l *SQLLinkIterator) buildSQL(next bool, val graph.Value) (string, []string) {
	query := "SELECT "
	t := []string{
		fmt.Sprintf("%s.subject", l.tableName),
		fmt.Sprintf("%s.predicate", l.tableName),
		fmt.Sprintf("%s.object", l.tableName),
		fmt.Sprintf("%s.label", l.tableName),
	}
	for _, v := range l.getTags() {
		t = append(t, fmt.Sprintf("%s.%s as %s", v.table, v.dir, v.tag))
	}
	query += strings.Join(t, ", ")
	query += " FROM "
	t = []string{}
	for _, k := range l.getTables() {
		t = append(t, fmt.Sprintf("quads as %s", k))
	}
	query += strings.Join(t, ", ")
	query += " WHERE "
	l.next = next
	constraint, values := l.buildWhere()

	if !next {
		v := val.(quad.Quad)
		if constraint != "" {
			constraint += " AND "
		}
		t = []string{
			fmt.Sprintf("%s.subject = ?", l.tableName),
			fmt.Sprintf("%s.predicate = ?", l.tableName),
			fmt.Sprintf("%s.object = ?", l.tableName),
			fmt.Sprintf("%s.label = ?", l.tableName),
		}
		constraint += strings.Join(t, " AND ")
		values = append(values, v.Subject)
		values = append(values, v.Predicate)
		values = append(values, v.Object)
		values = append(values, v.Label)
	}
	query += constraint
	query += ";"

	glog.V(2).Infoln(query)

	if glog.V(4) {
		dstr := query
		for i := 1; i <= len(values); i++ {
			dstr = strings.Replace(dstr, "?", fmt.Sprintf("'%s'", values[i-1]), 1)
		}
		glog.V(4).Infoln(dstr)
	}
	return query, values
}

func convertToPostgres(query string, values []string) string {
	for i := 1; i <= len(values); i++ {
		query = strings.Replace(query, "?", fmt.Sprintf("$%d", i), 1)
	}
	return query
}

func (l *SQLLinkIterator) makeCursor(next bool, value graph.Value) error {
	if l.cursor != nil {
		l.cursor.Close()
	}
	var q string
	var values []string
	q, values = l.buildSQL(next, value)
	q = convertToPostgres(q, values)
	ivalues := make([]interface{}, 0, len(values))
	for _, v := range values {
		ivalues = append(ivalues, v)
	}
	cursor, err := l.qs.db.Query(q, ivalues...)
	if err != nil {
		glog.Errorf("Couldn't get cursor from SQL database: %v", err)
		cursor = nil
		return err
	}
	l.cursor = cursor
	return nil
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

func (l *SQLLinkIterator) Next() bool {
	var err error
	graph.NextLogIn(l)
	if l.cursor == nil {
		err = l.makeCursor(true, nil)
		l.cols, err = l.cursor.Columns()
		if err != nil {
			glog.Errorf("Couldn't get columns")
			l.err = err
			l.cursor.Close()
			return false
		}
		// iterate the first one
		if !l.cursor.Next() {
			glog.V(4).Infoln("sql: No next")
			err := l.cursor.Err()
			if err != nil {
				glog.Errorf("Cursor error in SQL: %v", err)
				l.err = err
			}
			l.cursor.Close()
			return false
		}
		s, err := scan(l.cursor, len(l.cols))
		if err != nil {
			l.err = err
			l.cursor.Close()
			return false
		}
		l.resultNext = append(l.resultNext, s)
	}
	if l.resultList != nil && l.resultNext == nil {
		// We're on something and there's no next
		return false
	}
	l.resultList = l.resultNext
	l.resultNext = nil
	l.resultIndex = 0
	for {
		if !l.cursor.Next() {
			glog.V(4).Infoln("sql: No next")
			err := l.cursor.Err()
			if err != nil {
				glog.Errorf("Cursor error in SQL: %v", err)
				l.err = err
			}
			l.cursor.Close()
			break
		}
		s, err := scan(l.cursor, len(l.cols))
		if err != nil {
			l.err = err
			l.cursor.Close()
			return false
		}
		if l.resultList[0][0] == s[0] && l.resultList[0][1] == s[1] && l.resultList[0][2] == s[2] && l.resultList[0][3] == s[3] {
			l.resultList = append(l.resultList, s)
		} else {
			l.resultNext = append(l.resultNext, s)
			break
		}

	}
	if len(l.resultList) == 0 {
		return graph.NextLogOut(l, nil, false)
	}
	l.buildResult(0)
	return graph.NextLogOut(l, l.Result(), true)
}

type SQLAllIterator struct {
	// TBD
}
