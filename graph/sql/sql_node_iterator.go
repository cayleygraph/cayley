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

	"github.com/codelingo/cayley/clog"
	"github.com/codelingo/cayley/graph"
	"github.com/codelingo/cayley/quad"
)

var sqlNodeTableID uint64

type sqlQueryType int

const (
	node sqlQueryType = iota
	link
	nodeIntersect
)

func init() {
	atomic.StoreUint64(&sqlNodeTableID, 0)
}

func newNodeTableName() string {
	id := atomic.AddUint64(&sqlNodeTableID, 1)
	return fmt.Sprintf("n_%d", id)
}

type SQLNodeIterator struct {
	tableName string

	linkIt   sqlItDir
	size     int64
	tagger   graph.Tagger
	fixedSet []quad.Value

	result graph.Value
}

func (n *SQLNodeIterator) sqlClone() sqlIterator {
	m := &SQLNodeIterator{
		tableName: n.tableName,
		size:      n.size,
		linkIt: sqlItDir{
			dir: n.linkIt.dir,
			it:  n.linkIt.it.sqlClone(),
		},
		fixedSet: make([]quad.Value, len(n.fixedSet)),
	}
	m.tagger.CopyFromTagger(n.Tagger())
	copy(m.fixedSet, n.fixedSet)
	return m
}

func (n *SQLNodeIterator) Tagger() *graph.Tagger {
	return &n.tagger
}

func (n *SQLNodeIterator) Result() graph.Value {
	return n.result
}

func (n *SQLNodeIterator) Type() sqlQueryType {
	return node
}

func (n *SQLNodeIterator) Size(qs *QuadStore) (int64, bool) {
	return qs.Size() / 2, false
}

func (n *SQLNodeIterator) Describe() string {
	s, _ := n.buildSQL(true, nil)
	return fmt.Sprintf("SQL_NODE_QUERY: %s", s)
}

func (n *SQLNodeIterator) buildResult(result []NodeHash, cols []string) map[string]graph.Value {
	m := make(map[string]graph.Value)
	for i, c := range cols {
		if strings.HasSuffix(c, "_hash") {
			continue
		}
		if c == "__execd" {
			n.result = NodeHash(result[i])
		}
		m[c] = NodeHash(result[i])
	}
	return m
}

func (n *SQLNodeIterator) getTables() []tableDef {
	var out []tableDef
	if n.linkIt.it != nil {
		out = n.linkIt.it.getTables()
	}
	if len(out) == 0 {
		out = append(out, tableDef{table: "quads", name: n.tableName})
	}
	return out
}

func (n *SQLNodeIterator) tableID() tagDir {
	if n.linkIt.it != nil {
		return tagDir{
			table: n.linkIt.it.tableID().table,
			dir:   n.linkIt.dir,
			tag:   "__execd",
		}
	}
	return tagDir{
		table: n.tableName,
		dir:   quad.Any,
		tag:   "__execd",
	}
}

func (n *SQLNodeIterator) getLocalTags() []tagDir {
	myTag := n.tableID()
	var out []tagDir
	for _, tag := range n.tagger.Tags() {
		out = append(out, tagDir{
			dir:       myTag.dir,
			table:     myTag.table,
			tag:       tag,
			justLocal: true,
		})
	}
	return out
}

func (n *SQLNodeIterator) getTags() []tagDir {
	out := n.getLocalTags()
	if n.linkIt.it != nil {
		out = append(out, n.linkIt.it.getTags()...)
	}
	return out
}

func (n *SQLNodeIterator) buildWhere() (string, sqlArgs) {
	var q []string
	var vals sqlArgs
	if n.linkIt.it != nil {
		s, v := n.linkIt.it.buildWhere()
		q = append(q, s)
		vals = append(vals, v...)
	}
	if len(n.fixedSet) != 0 {
		topData := n.tableID()
		var valueChain []string
		for _, v := range n.fixedSet {
			vals = append(vals, hashOf(v).toSQL())
			valueChain = append(valueChain, "?")
		}
		q = append(q, fmt.Sprintf("%s.%s_hash IN (%s)", topData.table, topData.dir, strings.Join(valueChain, ", ")))
	}
	query := strings.Join(q, " AND ")
	return query, vals
}

func (n *SQLNodeIterator) buildSQL(next bool, val graph.Value) (string, sqlArgs) {
	topData := n.tableID()
	tags := []tagDir{topData}
	tags = append(tags, n.getTags()...)
	query := "SELECT "

	var t []string
	for _, v := range tags {
		t = append(t, v.String())
	}
	query += strings.Join(t, ", ")
	query += " FROM "
	t = []string{}
	var values sqlArgs
	for _, k := range n.getTables() {
		values = append(values, k.values...)
		t = append(t, fmt.Sprintf("%s as %s", k.table, k.name))
	}
	query += strings.Join(t, ", ")
	query += " WHERE "

	constraint, wherevalues := n.buildWhere()
	values = append(values, wherevalues...)

	if !next {
		v := val.(NodeHash)
		if constraint != "" {
			constraint += " AND "
		}
		constraint += fmt.Sprintf("%s.%s_hash = ?", topData.table, topData.dir)
		values = append(values, v.toSQL())
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

func (n *SQLNodeIterator) sameTopResult(target []NodeHash, test []NodeHash) bool {
	return target[0] == test[0]
}

func (n *SQLNodeIterator) quickContains(_ graph.Value) (bool, bool) { return false, false }
