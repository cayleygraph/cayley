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
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/query"
)

const Name = "mql"

func init() {
	query.RegisterLanguage(query.Language{
		Name: Name,
		Session: func(qs graph.QuadStore) query.Session {
			return NewSession(qs)
		},
		HTTP: func(qs graph.QuadStore) query.HTTP {
			return NewSession(qs)
		},
		REPL: func(qs graph.QuadStore) query.REPLSession {
			return NewSession(qs)
		},
	})
}

type Session struct {
	qs    graph.QuadStore
	query *Query
}

func NewSession(qs graph.QuadStore) *Session {
	return &Session{qs: qs}
}

func (s *Session) ShapeOf(query string) (interface{}, error) {
	var mqlQuery interface{}
	err := json.Unmarshal([]byte(query), &mqlQuery)
	if err != nil {
		return nil, err
	}
	s.query = NewQuery(s)
	s.query.BuildIteratorTree(mqlQuery)
	output := make(map[string]interface{})
	iterator.OutputQueryShapeForIterator(s.query.it, s.qs, output)
	nodes := make([]iterator.Node, 0)
	for _, n := range output["nodes"].([]iterator.Node) {
		n.Tags = nil
		nodes = append(nodes, n)
	}
	output["nodes"] = nodes
	return output, nil
}

func (s *Session) Execute(ctx context.Context, input string, c chan query.Result, limit int) {
	defer close(c)
	var mqlQuery interface{}
	if err := json.Unmarshal([]byte(input), &mqlQuery); err != nil {
		select {
		case c <- query.ErrorResult(err):
		case <-ctx.Done():
		}
		return
	}
	s.query = NewQuery(s)
	s.query.BuildIteratorTree(mqlQuery)
	if s.query.isError() {
		select {
		case c <- query.ErrorResult(s.query.err):
		case <-ctx.Done():
		}
		return
	}

	it := s.query.it
	err := graph.Iterate(ctx, it).Limit(limit).TagEach(func(tags map[string]graph.Value) {
		select {
		case c <- query.TagMapResult(tags):
		case <-ctx.Done():
		}
	})
	if err != nil {
		select {
		case c <- query.ErrorResult(err):
		case <-ctx.Done():
		}
	}
}

func (s *Session) FormatREPL(result query.Result) string {
	tags, ok := result.Result().(map[string]graph.Value)
	if !ok {
		return ""
	}
	out := fmt.Sprintln("****")
	tagKeys := make([]string, len(tags))
	s.query.treeifyResult(tags)
	s.query.buildResults()
	r, _ := json.MarshalIndent(s.query.results, "", " ")
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

func (s *Session) Collate(result query.Result) {
	res, ok := result.Result().(map[string]graph.Value)
	if !ok {
		return
	}
	s.query.treeifyResult(res)
}

func (s *Session) Results() (interface{}, error) {
	s.query.buildResults()
	if s.query.isError() {
		return nil, s.query.err
	}
	return s.query.results, nil
}

func (s *Session) Clear() {
	// Since we create a new Query underneath every query, clearing isn't necessary.
	return
}
