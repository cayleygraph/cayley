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
	"sort"

	"github.com/barakmich/glog"

	"github.com/google/cayley/graph"
)

type Session struct {
	ts           graph.TripleStore
	currentQuery *Query
	debug        bool
}

func NewSession(ts graph.TripleStore) *Session {
	var m Session
	m.ts = ts
	return &m
}

func (m *Session) ToggleDebug() {
	m.debug = !m.debug
}

func (m *Session) GetQuery(input string, output_struct chan map[string]interface{}) {
	defer close(output_struct)
	var mqlQuery interface{}
	err := json.Unmarshal([]byte(input), &mqlQuery)
	if err != nil {
		return
	}
	m.currentQuery = NewQuery(m)
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

func (s *Session) InputParses(input string) (graph.ParseResult, error) {
	var x interface{}
	err := json.Unmarshal([]byte(input), &x)
	if err != nil {
		return graph.ParseFail, err
	}
	return graph.Parsed, nil
}

func (s *Session) ExecInput(input string, c chan interface{}, limit int) {
	defer close(c)
	var mqlQuery interface{}
	err := json.Unmarshal([]byte(input), &mqlQuery)
	if err != nil {
		return
	}
	s.currentQuery = NewQuery(s)
	s.currentQuery.BuildIteratorTree(mqlQuery)
	if s.currentQuery.isError {
		return
	}
	it, _ := s.currentQuery.it.Optimize()
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

func (s *Session) ToText(result interface{}) string {
	tags := *(result.(*map[string]graph.TSVal))
	out := fmt.Sprintln("****")
	tagKeys := make([]string, len(tags))
	s.currentQuery.treeifyResult(tags)
	s.currentQuery.buildResults()
	r, _ := json.MarshalIndent(s.currentQuery.results, "", " ")
	fmt.Println(string(r))
	i := 0
	for k, _ := range tags {
		tagKeys[i] = string(k)
		i++
	}
	sort.Strings(tagKeys)
	for _, k := range tagKeys {
		if k == "$_" {
			continue
		}
		out += fmt.Sprintf("%s : %s\n", k, s.ts.GetNameFor(tags[k]))
	}
	return out
}

func (s *Session) BuildJson(result interface{}) {
	s.currentQuery.treeifyResult(*(result.(*map[string]graph.TSVal)))
}

func (s *Session) GetJson() (interface{}, error) {
	s.currentQuery.buildResults()
	if s.currentQuery.isError {
		return nil, s.currentQuery.err
	} else {
		return s.currentQuery.results, nil
	}
}

func (s *Session) ClearJson() {
	// Since we create a new Query underneath every query, clearing isn't necessary.
	return
}
