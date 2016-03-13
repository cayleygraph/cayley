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

	"database/sql"
	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
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
	dir    quad.Direction
	hashes []sql.NullString
}

type sqlItDir struct {
	dir quad.Direction
	it  sqlIterator
}

var _ sqlIterator = (*SQLLinkIterator)(nil)

type SQLLinkIterator struct {
	tagger graph.Tagger

	nodeIts     []sqlItDir
	constraints []constraint
	tableName   string
	size        int64
	tagdirs     []tagDir

	resultQuad QuadHashes
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
		l.size = qs.sizeForIterator(false, l.constraints[0].dir, l.constraints[0].hashes[0])
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
		desired := v.(QuadHashes).Get(c.dir)
		for _, s := range c.hashes {
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

func (l *SQLLinkIterator) buildResult(result []sql.NullString, cols []string) map[string]graph.Value {
	l.resultQuad = QuadHashes{
		result[0],
		result[1],
		result[2],
		result[3],
	}
	m := make(map[string]graph.Value)
	for i, c := range cols[4:] {
		m[c] = NodeHash(result[i+4])
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

func (l *SQLLinkIterator) buildWhere() (string, sqlArgs) {
	var q []string
	var vals sqlArgs
	for _, c := range l.constraints {
		if len(c.hashes) == 1 {
			q = append(q, fmt.Sprintf("%s.%s_hash = ?", l.tableName, c.dir))
			vals = append(vals, c.hashes[0])
		} else if len(c.hashes) > 1 {
			valslots := strings.Join(strings.Split(strings.Repeat("?", len(c.hashes)), ""), ", ")
			subq := fmt.Sprintf("%s.%s_hash IN (%s)", l.tableName, c.dir, valslots)
			q = append(q, subq)
			for _, v := range c.hashes {
				vals = append(vals, v)
			}
		}
	}
	for _, i := range l.nodeIts {
		t := i.it.tableID()
		dir := t.dir.String()
		if t.dir == quad.Any {
			dir = t.tag
		} else {
			dir += "_hash"
		}
		q = append(q, fmt.Sprintf("%s.%s_hash = %s.%s", l.tableName, i.dir, t.table, dir))
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

func (l *SQLLinkIterator) buildSQL(next bool, val graph.Value) (string, sqlArgs) {
	query := "SELECT "
	t := []string{
		fmt.Sprintf("%s.subject_hash AS subject", l.tableName),
		fmt.Sprintf("%s.predicate_hash AS predicate", l.tableName),
		fmt.Sprintf("%s.object_hash AS object", l.tableName),
		fmt.Sprintf("%s.label_hash AS label", l.tableName),
	}
	for _, v := range l.getTags() {
		t = append(t, v.String())
	}
	query += strings.Join(t, ", ")
	query += " FROM "
	t = []string{}
	var values sqlArgs
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
		h := val.(QuadHashes)
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
		values = append(values, h[0])
		values = append(values, h[1])
		values = append(values, h[2])
		values = append(values, h[3])
	}
	query += constraint
	query += ";"

	if clog.V(4) {
		dstr := query
		for i := 1; i <= len(values); i++ {
			dstr = strings.Replace(dstr, "?", fmt.Sprintf("'%s'", values[i-1]), 1)
		}
		clog.Infof("%v", dstr)
	}
	return query, values
}

func (l *SQLLinkIterator) sameTopResult(target []sql.NullString, test []sql.NullString) bool {
	return target[0] == test[0] && target[1] == test[1] && target[2] == test[2] && target[3] == test[3]
}
