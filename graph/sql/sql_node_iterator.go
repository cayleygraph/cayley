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

var sqlNodeTableID uint64

type sqlQueryType int

const (
	node sqlQueryType = iota
	link
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

	linkIts    []sqlItDir
	nodetables []string
	size       int64
	tagger     graph.Tagger

	result string
}

func (n *SQLNodeIterator) sqlClone() sqlIterator {
	m := &SQLNodeIterator{
		tableName: n.tableName,
		size:      n.size,
	}
	for _, i := range n.linkIts {
		m.linkIts = append(m.linkIts, sqlItDir{
			dir: i.dir,
			it:  i.it.sqlClone(),
		})
	}
	m.tagger.CopyFromTagger(n.Tagger())
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
	return qs.Size() / int64(len(n.linkIts)+1), true
}

func (n *SQLNodeIterator) Describe() string {
	return fmt.Sprintf("SQL_NODE_QUERY: %#v", n)
}

func (n *SQLNodeIterator) buildResult(result []string, cols []string) map[string]string {
	m := make(map[string]string)
	for i, c := range cols {
		if c == "__execd" {
			n.result = result[i]
		}
		m[c] = result[i]
	}
	return m
}

func (n *SQLNodeIterator) makeNodeTableNames() {
	if n.nodetables != nil {
		return
	}
	n.nodetables = make([]string, len(n.linkIts))
	for i, _ := range n.nodetables {
		n.nodetables[i] = newNodeTableName()
	}
}

func (n *SQLNodeIterator) getTables() []tableDef {
	var out []tableDef
	switch len(n.linkIts) {
	case 0:
		return []tableDef{tableDef{table: "quads", name: n.tableName}}
	case 1:
		out = n.linkIts[0].it.getTables()
	default:
		return n.buildSubqueries()
	}
	if len(out) == 0 {
		out = append(out, tableDef{table: "quads", name: n.tableName})
	}
	return out
}

func (n *SQLNodeIterator) buildSubqueries() []tableDef {
	var out []tableDef
	n.makeNodeTableNames()
	for i, it := range n.linkIts {
		var td tableDef
		// TODO(barakmich): This is a dirty hack. The real implementation is to
		// separate SQL iterators to build a similar tree as we're doing here, and
		// have a single graph.Iterator 'caddy' structure around it.
		subNode := &SQLNodeIterator{
			tableName: newTableName(),
			linkIts:   []sqlItDir{it},
		}
		var table string
		table, td.values = subNode.buildSQL(true, nil)
		td.table = fmt.Sprintf("\n(%s)", table[:len(table)-1])
		td.name = n.nodetables[i]
		out = append(out, td)
	}
	return out
}

func (n *SQLNodeIterator) tableID() tagDir {
	switch len(n.linkIts) {
	case 0:
		return tagDir{
			table: n.tableName,
			dir:   quad.Any,
			tag:   "__execd",
		}
	case 1:
		return tagDir{
			table: n.linkIts[0].it.tableID().table,
			dir:   n.linkIts[0].dir,
			tag:   "__execd",
		}
	default:
		n.makeNodeTableNames()
		return tagDir{
			table: n.nodetables[0],
			dir:   quad.Any,
			tag:   "__execd",
		}
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
	if len(n.linkIts) > 1 {
		n.makeNodeTableNames()
		for i, it := range n.linkIts {
			for _, v := range it.it.getTags() {
				out = append(out, tagDir{
					tag:   v.tag,
					dir:   quad.Any,
					table: n.nodetables[i],
				})
			}
		}
		return out
	}
	for _, i := range n.linkIts {
		out = append(out, i.it.getTags()...)
	}
	return out
}

func (n *SQLNodeIterator) buildWhere() (string, []string) {
	var q []string
	var vals []string
	if len(n.linkIts) > 1 {
		for _, tb := range n.nodetables[1:] {
			q = append(q, fmt.Sprintf("%s.__execd = %s.__execd", n.nodetables[0], tb))
		}
	} else {
		for _, i := range n.linkIts {
			s, v := i.it.buildWhere()
			q = append(q, s)
			vals = append(vals, v...)
		}
	}
	query := strings.Join(q, " AND ")
	return query, vals
}

func (n *SQLNodeIterator) buildSQL(next bool, val graph.Value) (string, []string) {
	topData := n.tableID()
	tags := []tagDir{topData}
	tags = append(tags, n.getTags()...)
	query := "SELECT DISTINCT "
	var t []string
	for _, v := range tags {
		t = append(t, v.String())
	}
	query += strings.Join(t, ", ")
	query += " FROM "
	t = []string{}
	var values []string
	for _, k := range n.getTables() {
		values = append(values, k.values...)
		t = append(t, fmt.Sprintf("%s as %s", k.table, k.name))
	}
	query += strings.Join(t, ", ")
	query += " WHERE "

	constraint, wherevalues := n.buildWhere()
	values = append(values, wherevalues...)

	if !next {
		v := val.(string)
		if constraint != "" {
			constraint += " AND "
		}
		constraint += fmt.Sprintf("%s.%s = ?", topData.table, topData.dir)
		values = append(values, v)
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

func (n *SQLNodeIterator) sameTopResult(target []string, test []string) bool {
	return target[0] == test[0]
}

func (n *SQLNodeIterator) quickContains(_ graph.Value) (bool, bool) { return false, false }
