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
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/query"
)

type Session struct {
	qs           graph.QuadStore
	currentQuery *Query
	debug        bool
}

func NewSession(qs graph.QuadStore) *Session {
	var m Session
	m.qs = qs
	return &m
}

func (s *Session) ToggleDebug() {
	s.debug = !s.debug
}

func (s *Session) GetQuery(input string, out chan map[string]interface{}) {
	defer close(out)
	var mqlQuery interface{}
	err := json.Unmarshal([]byte(input), &mqlQuery)
	if err != nil {
		return
	}
	s.currentQuery = NewQuery(s)
	s.currentQuery.BuildIteratorTree(mqlQuery)
	output := make(map[string]interface{})
	iterator.OutputQueryShapeForIterator(s.currentQuery.it, s.qs, output)
	nodes := make([]iterator.Node, 0)
	for _, n := range output["nodes"].([]iterator.Node) {
		n.Tags = nil
		nodes = append(nodes, n)
	}
	output["nodes"] = nodes
	out <- output
}

func (s *Session) InputParses(input string) (query.ParseResult, error) {
	var x interface{}
	err := json.Unmarshal([]byte(input), &x)
	if err != nil {
		return query.ParseFail, err
	}
	return query.Parsed, nil
}

func (s *Session) ExecInput(input string, c chan interface{}, _ int) {
	defer close(c)
	var mqlQuery interface{}
	err := json.Unmarshal([]byte(input), &mqlQuery)
	if err != nil {
		return
	}
	s.currentQuery = NewQuery(s)
	s.currentQuery.BuildIteratorTree(mqlQuery)
	if s.currentQuery.isError() {
		return
	}
	it, _ := s.currentQuery.it.Optimize()
	if glog.V(2) {
		b, err := json.MarshalIndent(it.Describe(), "", "  ")
		if err != nil {
			glog.Infof("failed to format description: %v", err)
		} else {
			glog.Infof("%s", b)
		}
	}
	for graph.Next(it) {
		tags := make(map[string]graph.Value)
		it.TagResults(tags)
		c <- tags
		for it.NextPath() == true {
			tags := make(map[string]graph.Value)
			it.TagResults(tags)
			c <- tags
		}
	}
}

func (s *Session) ToText(result interface{}) string {
	tags := result.(map[string]graph.Value)
	out := fmt.Sprintln("****")
	tagKeys := make([]string, len(tags))
	s.currentQuery.treeifyResult(tags)
	s.currentQuery.buildResults()
	r, _ := json.MarshalIndent(s.currentQuery.results, "", " ")
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
		out += fmt.Sprintf("%s : %s\n", k, s.qs.NameOf(tags[k]))
	}
	return out
}

func (s *Session) BuildJSON(result interface{}) {
	s.currentQuery.treeifyResult(result.(map[string]graph.Value))
}

func (s *Session) GetJSON() ([]interface{}, error) {
	s.currentQuery.buildResults()
	if s.currentQuery.isError() {
		return nil, s.currentQuery.err
	}
	return s.currentQuery.results, nil
}

func (s *Session) ClearJSON() {
	// Since we create a new Query underneath every query, clearing isn't necessary.
	return
}
