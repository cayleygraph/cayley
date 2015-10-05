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
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/barakmich/glog"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/quad"
)

var sqlTableID uint64

func init() {
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
	tag       string
	dir       quad.Direction
	table     string
	justLocal bool
}

func (t tagDir) String() string {
	if t.dir == quad.Any {
		if t.justLocal {
			return fmt.Sprintf("%s.__execd as \"%s\", %s.__execd_hash as %s_hash", t.table, t.tag, t.table, t.tag)
		}
		return fmt.Sprintf("%s.\"%s\" as \"%s\", %s.%s_hash as %s_hash", t.table, t.tag, t.tag, t.table, t.tag, t.tag)
	}
	return fmt.Sprintf("%s.%s as \"%s\", %s.%s_hash as %s_hash", t.table, t.dir, t.tag, t.table, t.dir, t.tag)
}

type tableDef struct {
	table  string
	name   string
	values []string
}

type sqlItDir struct {
	dir quad.Direction
	it  sqlIterator
}

type sqlIterator interface {
	sqlClone() sqlIterator

	buildSQL(next bool, val graph.Value) (string, []string)
	getTables() []tableDef
	getTags() []tagDir
	buildWhere() (string, []string)
	tableID() tagDir

	quickContains(graph.Value) (ok bool, result bool)
	buildResult(result []string, cols []string) map[string]string
	sameTopResult(target []string, test []string) bool

	Result() graph.Value
	Size(*QuadStore) (int64, bool)
	Describe() string
	Type() sqlQueryType
	Tagger() *graph.Tagger
}

type SQLLinkIterator struct {
	tagger graph.Tagger

	nodeIts     []sqlItDir
	constraints []constraint
	tableName   string
	size        int64
	tagdirs     []tagDir

	resultQuad quad.Quad
}

func (l *SQLLinkIterator) sqlClone() sqlIterator {
	m := &SQLLinkIterator{
		tableName:   l.tableName,
		size:        l.size,
		constraints: make([]constraint, len(l.constraints)),
		tagdirs:     make([]tagDir, len(l.tagdirs)),
	}
	for _, i := range l.nodeIts {
		m.nodeIts = append(m.nodeIts, sqlItDir{
			dir: i.dir,
			it:  i.it.sqlClone(),
		})
	}
	copy(m.constraints, l.constraints)
	copy(m.tagdirs, l.tagdirs)
	m.tagger.CopyFromTagger(l.Tagger())
	return m
}

func (l *SQLLinkIterator) Tagger() *graph.Tagger {
	return &l.tagger
}

func (l *SQLLinkIterator) Result() graph.Value {
	return l.resultQuad
}

func (l *SQLLinkIterator) Size(qs *QuadStore) (int64, bool) {
	if l.size != 0 {
		return l.size, true
	}
	if len(l.constraints) > 0 {
		l.size = qs.sizeForIterator(false, l.constraints[0].dir, l.constraints[0].vals[0])
	} else if len(l.nodeIts) > 1 {
		subsize, _ := l.nodeIts[0].it.(*SQLNodeIterator).Size(qs)
		return subsize * 20, false
	} else {
		return qs.Size(), false
	}
	return l.size, true
}

func (l *SQLLinkIterator) Describe() string {
	s, _ := l.buildSQL(true, nil)
	return fmt.Sprintf("SQL_LINK_QUERY: %s", s)
}

func (l *SQLLinkIterator) Type() sqlQueryType {
	return link
}

func (l *SQLLinkIterator) quickContains(v graph.Value) (bool, bool) {
	for _, c := range l.constraints {
		none := true
		desired := v.(quad.Quad).Get(c.dir)
		for _, s := range c.vals {
			if s == desired {
				none = false
				break
			}
		}
		if none {
			return true, false
		}
	}
	if len(l.nodeIts) == 0 {
		return true, true
	}
	return false, false
}

func (l *SQLLinkIterator) buildResult(result []string, cols []string) map[string]string {
	var q quad.Quad
	q.Subject = result[0]
	q.Predicate = result[1]
	q.Object = result[2]
	q.Label = result[3]
	l.resultQuad = q
	m := make(map[string]string)
	for i, c := range cols[4:] {
		m[c] = result[i+4]
	}
	return m
}

func (l *SQLLinkIterator) getTables() []tableDef {
	out := []tableDef{tableDef{table: "quads", name: l.tableName}}
	for _, i := range l.nodeIts {
		out = append(out, i.it.getTables()...)
	}
	return out
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
	for _, tag := range l.tagdirs {
		out = append(out, tagDir{
			dir:   tag.dir,
			table: l.tableName,
			tag:   tag.tag,
		})

	}
	for _, i := range l.nodeIts {
		out = append(out, i.it.getTags()...)
	}
	return out
}

func (l *SQLLinkIterator) buildWhere() (string, []string) {
	var q []string
	var vals []string
	for _, c := range l.constraints {
		q = append(q, fmt.Sprintf("%s.%s_hash = ?", l.tableName, c.dir))
		vals = append(vals, hashOf(c.vals[0]))
	}
	for _, i := range l.nodeIts {
		t := i.it.tableID()
		dir := t.dir.String()
		if t.dir == quad.Any {
			dir = t.tag
		}
		q = append(q, fmt.Sprintf("%s.%s_hash = %s.%s_hash", l.tableName, i.dir, t.table, dir))
	}
	for _, i := range l.nodeIts {
		s, v := i.it.buildWhere()
		q = append(q, s)
		vals = append(vals, v...)
	}
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
		t = append(t, v.String())
	}
	query += strings.Join(t, ", ")
	query += " FROM "
	t = []string{}
	var values []string
	for _, k := range l.getTables() {
		values = append(values, k.values...)
		t = append(t, fmt.Sprintf("%s as %s", k.table, k.name))
	}
	query += strings.Join(t, ", ")
	constraint, wherevalues := l.buildWhere()
	if constraint != "" {
		query += " WHERE "
	}

	values = append(values, wherevalues...)
	if !next {
		v := val.(quad.Quad)
		if constraint != "" {
			constraint += " AND "
		} else {
			constraint += " WHERE "
		}
		t = []string{
			fmt.Sprintf("%s.subject_hash = ?", l.tableName),
			fmt.Sprintf("%s.predicate_hash = ?", l.tableName),
			fmt.Sprintf("%s.object_hash = ?", l.tableName),
			fmt.Sprintf("%s.label_hash = ?", l.tableName),
		}
		constraint += strings.Join(t, " AND ")
		values = append(values, hashOf(v.Subject))
		values = append(values, hashOf(v.Predicate))
		values = append(values, hashOf(v.Object))
		values = append(values, hashOf(v.Label))
	}
	query += constraint
	query += ";"

	if glog.V(4) {
		dstr := query
		for i := 1; i <= len(values); i++ {
			dstr = strings.Replace(dstr, "?", fmt.Sprintf("'%s'", values[i-1]), 1)
		}
		glog.V(4).Infoln(dstr)
	}
	return query, values
}

func (l *SQLLinkIterator) sameTopResult(target []string, test []string) bool {
	return target[0] == test[0] && target[1] == test[1] && target[2] == test[2] && target[3] == test[3]
}
