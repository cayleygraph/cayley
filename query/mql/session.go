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
	qs graph.QuadStore
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
	q := NewQuery(s)
	q.BuildIteratorTree(mqlQuery)
	output := make(map[string]interface{})
	iterator.OutputQueryShapeForIterator(q.it, s.qs, output)
	nodes := make([]iterator.Node, 0)
	for _, n := range output["nodes"].([]iterator.Node) {
		n.Tags = nil
		nodes = append(nodes, n)
	}
	output["nodes"] = nodes
	return output, nil
}

type mqlIterator struct {
	q   *Query
	col query.Collation
	it  graph.Iterator
	res []interface{}
}

func (it *mqlIterator) Next(ctx context.Context) bool {
	// TODO: stream results
	if it.res != nil {
		if len(it.res) == 0 {
			return false
		}
		it.res = it.res[1:]
		return len(it.res) != 0
	}
	for it.it.Next(ctx) {
		m := make(map[string]graph.Ref)
		it.it.TagResults(m)
		it.q.treeifyResult(m)
		for it.it.NextPath(ctx) {
			m = make(map[string]graph.Ref, len(m))
			it.it.TagResults(m)
			it.q.treeifyResult(m)
		}
	}
	if err := it.it.Err(); err != nil {
		return false
	}
	it.q.buildResults()
	it.res = it.q.results
	return len(it.res) != 0
}

func (it *mqlIterator) Result() interface{} {
	if len(it.res) == 0 {
		return nil
	}
	return it.res[0]
}

func (it *mqlIterator) Err() error {
	return it.it.Err()
}

func (it *mqlIterator) Close() error {
	return it.it.Close()
}

func (s *Session) Execute(ctx context.Context, input string, opt query.Options) (query.Iterator, error) {
	switch opt.Collation {
	case query.REPL, query.JSON:
	default:
		return nil, &query.ErrUnsupportedCollation{Collation: opt.Collation}
	}
	var mqlQuery interface{}
	if err := json.Unmarshal([]byte(input), &mqlQuery); err != nil {
		return nil, err
	}
	q := NewQuery(s)
	q.BuildIteratorTree(mqlQuery)
	if q.isError() {
		return nil, q.err
	}

	it := q.it
	if opt.Limit > 0 {
		it = iterator.NewLimit(it, int64(opt.Limit))
	}
	return &mqlIterator{
		q:   q,
		col: opt.Collation,
		it:  it,
	}, nil
}

func (s *Session) Clear() {
	// Since we create a new Query underneath every query, clearing isn't necessary.
	return
}
