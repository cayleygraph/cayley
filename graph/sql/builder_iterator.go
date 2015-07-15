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

var sqlBuilderType graph.Type

func init() {
	sqlBuilderType = graph.RegisterIterator("sqlbuilder")
}

type tableDir struct {
	table string
	dir   quad.Direction
}

func (td tableDir) String() string {
	if td.table != "" {
		return fmt.Sprintf("%s.%s", td.table, td.dir)
	}
	return "ERR"
}

type clause interface {
	toSQL() (string, []string)
	getTables() map[string]bool
}

type baseClause struct {
	pair      tableDir
	strTarget []string
	target    tableDir
}

func (b baseClause) toSQL() (string, []string) {
	if len(b.strTarget) > 1 {
		// TODO(barakmich): Sets of things, IN clause
		return "", []string{}
	}
	if len(b.strTarget) == 0 {
		return fmt.Sprintf("%s = %s", b.pair, b.target), []string{}
	}
	return fmt.Sprintf("%s = ?", b.pair), []string{b.strTarget[0]}
}

func (b baseClause) getTables() map[string]bool {
	out := make(map[string]bool)
	if b.pair.table != "" {
		out[b.pair.table] = true
	}
	if b.target.table != "" {
		out[b.target.table] = true
	}
	return out
}

type joinClause struct {
	left  clause
	right clause
	op    clauseOp
}

func (jc joinClause) toSQL() (string, []string) {
	l, lstr := jc.left.toSQL()
	r, rstr := jc.right.toSQL()
	lstr = append(lstr, rstr...)
	var op string
	switch jc.op {
	case andClause:
		op = "AND"
	case orClause:
		op = "OR"
	}
	return fmt.Sprint("(%s %s %s)", l, op, r), lstr
}

func (jc joinClause) getTables() map[string]bool {
	m := jc.left.getTables()
	for k, _ := range jc.right.getTables() {
		m[k] = true
	}
	return m
}

type tag struct {
	pair tableDir
	t    string
}

type statementType int

const (
	node statementType = iota
	link
)

type clauseOp int

const (
	andClause clauseOp = iota
	orClause
)

func (it *StatementIterator) canonicalizeWhere() (string, []string) {
	var out []string
	var values []string
	for _, b := range it.buildWhere {
		b.pair.table = it.tableName()
		s, v := b.toSQL()
		values = append(values, v...)
		out = append(out, s)
	}
	return strings.Join(out, " AND "), values
}

func (it *StatementIterator) getTables() map[string]bool {
	m := make(map[string]bool)
	if it.where != nil {
		m = it.where.getTables()
	}
	for _, t := range it.tags {
		if t.pair.table != "" {
			m[t.pair.table] = true
		}
	}
	return m
}

func (it *StatementIterator) tableName() string {
	return fmt.Sprintf("t_%d", it.uid)
}

func (it *StatementIterator) buildQuery(contains bool, v graph.Value) (string, []string) {
	str := "SELECT "
	var t []string
	if it.stType == link {
		t = []string{
			fmt.Sprintf("%s.subject", it.tableName()),
			fmt.Sprintf("%s.predicate", it.tableName()),
			fmt.Sprintf("%s.object", it.tableName()),
			fmt.Sprintf("%s.label", it.tableName()),
		}
	} else {
		t = []string{fmt.Sprintf("%s.%s as __execd", it.tableName(), it.dir)}
	}
	for _, v := range it.tags {
		t = append(t, fmt.Sprintf("%s as %s", v.pair, v.t))
	}
	str += strings.Join(t, ", ")
	str += " FROM "
	t = []string{fmt.Sprintf("quads as %s", it.tableName())}
	for k, _ := range it.getTables() {
		if k != it.tableName() {
			t = append(t, fmt.Sprintf("quads as %s", k))
		}
	}
	str += strings.Join(t, ", ")
	str += " WHERE "
	var values []string
	var s string
	if it.stType != node {
		s, values = it.canonicalizeWhere()
	}
	if it.where != nil {
		if s != "" {
			s += " AND "
		}
		where, v2 := it.where.toSQL()
		s += where
		values = append(values, v2...)
	}
	str += s
	if contains {
		if it.stType == link {
			q := v.(quad.Quad)
			str += " AND "
			t = []string{
				fmt.Sprintf("%s.subject = ?", it.tableName()),
				fmt.Sprintf("%s.predicate = ?", it.tableName()),
				fmt.Sprintf("%s.object = ?", it.tableName()),
				fmt.Sprintf("%s.label = ?", it.tableName()),
			}
			str += " " + strings.Join(t, " AND ") + " "
			values = append(values, q.Subject)
			values = append(values, q.Predicate)
			values = append(values, q.Object)
			values = append(values, q.Label)
		} else {
			str += fmt.Sprintf(" AND %s.%s = ? ", it.tableName(), it.dir)
			values = append(values, v.(string))
		}

	}
	if it.stType == node {
		str += " ORDER BY __execd "
	}
	str += ";"
	for i := 1; i <= len(values); i++ {
		str = strings.Replace(str, "?", fmt.Sprintf("$%d", i), 1)
	}
	return str, values
}

type StatementIterator struct {
	uid uint64
	qs  *QuadStore

	// Only for links
	buildWhere []baseClause

	where       clause
	tagger      graph.Tagger
	tags        []tag
	err         error
	cursor      *sql.Rows
	stType      statementType
	dir         quad.Direction
	result      map[string]string
	resultIndex int
	resultList  [][]string
	resultNext  [][]string
	cols        []string
	resultQuad  quad.Quad
	size        int64
}

func (it *StatementIterator) Clone() graph.Iterator {
	m := &StatementIterator{
		uid:        iterator.NextUID(),
		qs:         it.qs,
		buildWhere: it.buildWhere,
		where:      it.where,
		stType:     it.stType,
		size:       it.size,
	}
	copy(it.tags, m.tags)
	m.tagger.CopyFrom(it)
	return m
}

func NewStatementIterator(qs *QuadStore, d quad.Direction, val string) *StatementIterator {
	it := &StatementIterator{
		uid: iterator.NextUID(),
		qs:  qs,
		buildWhere: []baseClause{
			baseClause{
				pair:      tableDir{"", d},
				strTarget: []string{val},
			},
		},
		stType: link,
		size:   -1,
	}
	return it
}

func (it *StatementIterator) UID() uint64 {
	return it.uid
}

func (it *StatementIterator) Reset() {
	it.err = nil
	it.Close()
}

func (it *StatementIterator) Err() error {
	return it.err
}

func (it *StatementIterator) Close() error {
	if it.cursor != nil {
		err := it.cursor.Close()
		if err != nil {
			return err
		}
		it.cursor = nil
	}
	return nil
}

func (it *StatementIterator) Tagger() *graph.Tagger {
	return &it.tagger
}

func (it *StatementIterator) Result() graph.Value {
	if it.stType == node {
		return it.result["__execd"]
	}
	return it.resultQuad
}

func (it *StatementIterator) TagResults(dst map[string]graph.Value) {
	for tag, value := range it.result {
		if tag == "__execd" {
			for _, tag := range it.tagger.Tags() {
				dst[tag] = value
			}
			continue
		}
		dst[tag] = value
	}

	for tag, value := range it.tagger.Fixed() {
		dst[tag] = value
	}
}

func (it *StatementIterator) Type() graph.Type {
	return sqlBuilderType
}

func (it *StatementIterator) preFilter(v graph.Value) bool {
	if it.stType == link {
		q := v.(quad.Quad)
		for _, b := range it.buildWhere {
			if len(b.strTarget) == 0 {
				continue
			}
			canFilter := true
			for _, s := range b.strTarget {
				if q.Get(b.pair.dir) == s {
					canFilter = false
					break
				}
			}
			if canFilter {
				return true
			}
		}
	}
	return false
}

func (it *StatementIterator) Contains(v graph.Value) bool {
	var err error
	if it.preFilter(v) {
		return false
	}
	q, values := it.buildQuery(true, v)
	ivalues := make([]interface{}, 0, len(values))
	for _, v := range values {
		ivalues = append(ivalues, v)
	}
	it.cursor, err = it.qs.db.Query(q, ivalues...)
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
		s, err := it.scan()
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

func (it *StatementIterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *StatementIterator) Sorted() bool                     { return false }
func (it *StatementIterator) Optimize() (graph.Iterator, bool) { return it, false }

func (it *StatementIterator) Size() (int64, bool) {

	if it.size != -1 {
		return it.size, true
	}
	if it.stType == node {
		return it.qs.Size(), true
	}
	b := it.buildWhere[0]
	it.size = it.qs.sizeForIterator(false, b.pair.dir, b.strTarget[0])
	return it.size, true
}

func (it *StatementIterator) Describe() graph.Description {
	size, _ := it.Size()
	return graph.Description{
		UID:  it.UID(),
		Name: "SQL_QUERY",
		Type: it.Type(),
		Size: size,
	}
}

func (it *StatementIterator) Stats() graph.IteratorStats {
	size, _ := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     5,
		Size:         size,
	}
}

func (it *StatementIterator) makeCursor() {
	if it.cursor != nil {
		it.cursor.Close()
	}
	q, values := it.buildQuery(false, nil)
	ivalues := make([]interface{}, 0, len(values))
	for _, v := range values {
		ivalues = append(ivalues, v)
	}
	cursor, err := it.qs.db.Query(q, ivalues...)
	if err != nil {
		glog.Errorln("Couldn't get cursor from SQL database: %v", err)
		cursor = nil
	}
	it.cursor = cursor
}

func (it *StatementIterator) NextPath() bool {
	it.resultIndex += 1
	if it.resultIndex >= len(it.resultList) {
		return false
	}
	it.buildResult(it.resultIndex)
	return true
}

func (it *StatementIterator) Next() bool {
	var err error
	graph.NextLogIn(it)
	if it.cursor == nil {
		it.makeCursor()
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
		s, err := it.scan()
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
		s, err := it.scan()
		if err != nil {
			it.err = err
			it.cursor.Close()
			return false
		}
		if it.stType == node {
			if it.resultList[0][0] != s[0] {
				it.resultNext = append(it.resultNext, s)
				break
			} else {
				it.resultList = append(it.resultList, s)
			}
		} else {
			if it.resultList[0][0] == s[0] && it.resultList[0][1] == s[1] && it.resultList[0][2] == s[2] && it.resultList[0][3] == s[3] {
				it.resultList = append(it.resultList, s)
			} else {
				it.resultNext = append(it.resultNext, s)
				break
			}
		}

	}
	if len(it.resultList) == 0 {
		return graph.NextLogOut(it, nil, false)
	}
	it.buildResult(0)
	return graph.NextLogOut(it, it.result, true)
}

func (it *StatementIterator) scan() ([]string, error) {
	pointers := make([]interface{}, len(it.cols))
	container := make([]string, len(it.cols))
	for i, _ := range pointers {
		pointers[i] = &container[i]
	}
	err := it.cursor.Scan(pointers...)
	if err != nil {
		glog.Errorf("Error scanning iterator: %v", err)
		it.err = err
		return nil, err
	}
	return container, nil
}

func (it *StatementIterator) buildResult(i int) {
	container := it.resultList[i]
	if it.stType == node {
		it.result = make(map[string]string)
		for i, c := range it.cols {
			it.result[c] = container[i]
		}
		return
	}
	var q quad.Quad
	q.Subject = container[0]
	q.Predicate = container[1]
	q.Object = container[2]
	q.Label = container[3]
	it.resultQuad = q
	it.result = make(map[string]string)
	for i, c := range it.cols[4:] {
		it.result[c] = container[i+4]
	}
}
