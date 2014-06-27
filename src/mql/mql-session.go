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

package mql

import (
	"encoding/json"
	"fmt"
	"github.com/barakmich/glog"
	"graph"
	"sort"
)

type MqlSession struct {
	ts           graph.TripleStore
	currentQuery *MqlQuery
	debug        bool
}

func NewMqlSession(ts graph.TripleStore) *MqlSession {
	var m MqlSession

	m.ts = ts

	return &m
}

func (m *MqlSession) ToggleDebug() {
	m.debug = !m.debug
}

func (m *MqlSession) GetQuery(input string, output_struct chan map[string]interface{}) {
	defer close(output_struct)

	var mqlQuery interface{}

	err := json.Unmarshal([]byte(input), &mqlQuery)

	if err != nil {
		return
	}

	m.currentQuery = NewMqlQuery(m)

	m.currentQuery.BuildIteratorTree(mqlQuery)

	output := make(map[string]interface{})

	graph.OutputQueryShapeForIterator(m.currentQuery.it, m.ts, &output)

	nodes := output["nodes"].([]graph.Node)
	new_nodes := make([]graph.Node, 0)

	for _, n := range nodes {
		n.Tags = nil
		new_nodes = append(new_nodes, n)
	}

	output["nodes"] = new_nodes

	output_struct <- output
}

func (m *MqlSession) InputParses(input string) (graph.ParseResult, error) {
	var x interface{}

	err := json.Unmarshal([]byte(input), &x)

	if err != nil {
		return graph.ParseFail, err
	}

	return graph.Parsed, nil
}

func (m *MqlSession) ExecInput(input string, c chan interface{}, limit int) {
	defer close(c)

	var mqlQuery interface{}

	err := json.Unmarshal([]byte(input), &mqlQuery)

	if err != nil {
		return
	}

	m.currentQuery = NewMqlQuery(m)

	m.currentQuery.BuildIteratorTree(mqlQuery)

	if m.currentQuery.isError {
		return
	}

	it, _ := m.currentQuery.it.Optimize()

	if glog.V(2) {
		glog.V(2).Infoln(it.DebugString(0))
	}

	for {
		_, ok := it.Next()

		if !ok {
			break
		}

		tags := make(map[string]graph.TSVal)

		it.TagResults(&tags)

		c <- &tags

		for it.NextResult() == true {
			tags := make(map[string]graph.TSVal)

			it.TagResults(&tags)

			c <- &tags
		}
	}
}

func (m *MqlSession) ToText(result interface{}) string {
	tags := *(result.(*map[string]graph.TSVal))
	out := fmt.Sprintln("****")
	tagKeys := make([]string, len(tags))

	m.currentQuery.treeifyResult(tags)
	m.currentQuery.buildResults()

	r, _ := json.MarshalIndent(m.currentQuery.results, "", " ")

	fmt.Println(string(r))

	i := 0

	for k := range tags {
		tagKeys[i] = string(k)

		i++
	}

	sort.Strings(tagKeys)

	for _, k := range tagKeys {
		if k == "$_" {
			continue
		}

		out += fmt.Sprintf("%s : %s\n", k, m.ts.GetNameFor(tags[k]))
	}

	return out
}

func (m *MqlSession) BuildJson(result interface{}) {
	m.currentQuery.treeifyResult(*(result.(*map[string]graph.TSVal)))
}

func (m *MqlSession) GetJson() (interface{}, error) {
	m.currentQuery.buildResults()

	if m.currentQuery.isError {
		return nil, m.currentQuery.err
	} else {
		return m.currentQuery.results, nil
	}
}

func (m *MqlSession) ClearJson() {
	// Since we create a new MqlQuery underneath every query, clearing isn't necessary.
	return
}
