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

	"golang.org/x/net/context"

	"github.com/codelingo/cayley/clog"
	"github.com/codelingo/cayley/graph"
	"github.com/codelingo/cayley/graph/iterator"
	"github.com/codelingo/cayley/query"
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

func (s *Session) Debug(ok bool) {
	s.debug = ok
}

func (s *Session) ShapeOf(query string) (interface{}, error) {
	var mqlQuery interface{}
	err := json.Unmarshal([]byte(query), &mqlQuery)
	if err != nil {
		return nil, err
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
	return output, nil
}

func (s *Session) Parse(input string) (query.ParseResult, error) {
	var x interface{}
	err := json.Unmarshal([]byte(input), &x)
	if err != nil {
		return query.ParseFail, err
	}
	return query.Parsed, nil
}

func (s *Session) Execute(input string, c chan interface{}, limit int) {
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

	it := s.currentQuery.it
	err = graph.Iterate(context.TODO(), it).Limit(limit).TagEach(func(tags map[string]graph.Value) {
		c <- tags
	})
	if err != nil {
		clog.Errorf("mql: %v", err)
	}
}

func (s *Session) Format(result interface{}) string {
	tags, ok := result.(map[string]graph.Value)
	if !ok {
		return ""
	}
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

func (s *Session) Collate(result interface{}) {
	res, ok := result.(map[string]graph.Value)
	if !ok {
		return
	}
	s.currentQuery.treeifyResult(res)
}

func (s *Session) Results() (interface{}, error) {
	s.currentQuery.buildResults()
	if s.currentQuery.isError() {
		return nil, s.currentQuery.err
	}
	return s.currentQuery.results, nil
}

func (s *Session) Clear() {
	// Since we create a new Query underneath every query, clearing isn't necessary.
	return
}
