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

	"github.com/barakmich/glog"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/quad"
)

type SQLNodeIntersection struct {
	tableName string

	nodeIts    []sqlIterator
	nodetables []string
	size       int64
	tagger     graph.Tagger

	result string
}

func (n *SQLNodeIntersection) sqlClone() sqlIterator {
	m := &SQLNodeIntersection{
		tableName: n.tableName,
		size:      n.size,
	}
	for _, i := range n.nodeIts {
		m.nodeIts = append(m.nodeIts, i.sqlClone())
	}
	m.tagger.CopyFromTagger(n.Tagger())
	return m
}

func (n *SQLNodeIntersection) Tagger() *graph.Tagger {
	return &n.tagger
}

func (n *SQLNodeIntersection) Result() graph.Value {
	return n.result
}

func (n *SQLNodeIntersection) Type() sqlQueryType {
	return nodeIntersect
}

func (n *SQLNodeIntersection) Size(qs *QuadStore) (int64, bool) {
	return qs.Size() / int64(len(n.nodeIts)+1), true
}

func (n *SQLNodeIntersection) Describe() string {
	s, _ := n.buildSQL(true, nil)
	return fmt.Sprintf("SQL_NODE_INTERSECTION: %s", s)
}

func (n *SQLNodeIntersection) buildResult(result []string, cols []string) map[string]string {
	m := make(map[string]string)
	for i, c := range cols {
		if strings.HasSuffix(c, "_hash") {
			continue
		}
		if c == "__execd" {
			n.result = result[i]
		}
		m[c] = result[i]
	}
	return m
}

func (n *SQLNodeIntersection) makeNodeTableNames() {
	if n.nodetables != nil {
		return
	}
	n.nodetables = make([]string, len(n.nodeIts))
	for i, _ := range n.nodetables {
		n.nodetables[i] = newNodeTableName()
	}
}

func (n *SQLNodeIntersection) getTables() []tableDef {
	if len(n.nodeIts) == 0 {
		panic("Combined no subnode queries")
	}
	return n.buildSubqueries()
}

func (n *SQLNodeIntersection) buildSubqueries() []tableDef {
	var out []tableDef
	n.makeNodeTableNames()
	for i, it := range n.nodeIts {
		var td tableDef
		var table string
		table, td.values = it.buildSQL(true, nil)
		td.table = fmt.Sprintf("\n(%s)", table[:len(table)-1])
		td.name = n.nodetables[i]
		out = append(out, td)
	}
	return out
}

func (n *SQLNodeIntersection) tableID() tagDir {
	n.makeNodeTableNames()
	return tagDir{
		table: n.nodetables[0],
		dir:   quad.Any,
		tag:   "__execd",
	}
}

func (n *SQLNodeIntersection) getLocalTags() []tagDir {
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

func (n *SQLNodeIntersection) getTags() []tagDir {
	out := n.getLocalTags()
	n.makeNodeTableNames()
	for i, it := range n.nodeIts {
		for _, v := range it.getTags() {
			out = append(out, tagDir{
				tag:   v.tag,
				dir:   quad.Any,
				table: n.nodetables[i],
			})
		}
	}
	return out
}

func (n *SQLNodeIntersection) buildWhere() (string, []string) {
	var q []string
	var vals []string
	for _, tb := range n.nodetables[1:] {
		q = append(q, fmt.Sprintf("%s.__execd_hash = %s.__execd_hash", n.nodetables[0], tb))
	}
	query := strings.Join(q, " AND ")
	return query, vals
}

func (n *SQLNodeIntersection) buildSQL(next bool, val graph.Value) (string, []string) {
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
		constraint += fmt.Sprintf("%s.%s_hash = ?", topData.table, topData.dir)
		values = append(values, hashOf(v))
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

func (n *SQLNodeIntersection) sameTopResult(target []string, test []string) bool {
	return target[0] == test[0]
}

func (n *SQLNodeIntersection) quickContains(_ graph.Value) (bool, bool) { return false, false }
