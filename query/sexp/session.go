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

package sexp

// Defines a running session of the sexp query language.

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/query"
)

const Name = "sexp"

func init() {
	query.RegisterLanguage(query.Language{
		Name: Name,
		Session: func(qs graph.QuadStore) query.Session {
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

func (s *Session) Parse(input string) error {
	var parenDepth int
	for i, x := range input {
		if x == '(' {
			parenDepth++
		}
		if x == ')' {
			parenDepth--
			if parenDepth < 0 {
				min := 0
				if (i - 10) > min {
					min = i - 10
				}
				return fmt.Errorf("too many close parentheses at char %d: %s", i, input[min:i])
			}
		}
	}
	if parenDepth > 0 {
		return query.ErrParseMore
	}
	if len(ParseString(input)) > 0 {
		return nil
	}
	return errors.New("invalid syntax")
}

func (s *Session) Execute(ctx context.Context, input string, opt query.Options) (query.Iterator, error) {
	switch opt.Collation {
	case query.Raw, query.REPL:
	default:
		return nil, &query.ErrUnsupportedCollation{Collation: opt.Collation}
	}
	it := BuildIteratorTreeForQuery(s.qs, input)
	if err := it.Err(); err != nil {
		return nil, err
	}
	if opt.Limit > 0 {
		it = iterator.NewLimit(it, int64(opt.Limit))
	}
	return &results{
		s:   s,
		col: opt.Collation,
		it:  it,
	}, nil
}

type results struct {
	s        *Session
	col      query.Collation
	it       graph.Iterator
	nextPath bool
}

func (it *results) Next(ctx context.Context) bool {
	if it.nextPath && it.it.NextPath(ctx) {
		return true
	}
	it.nextPath = false
	if it.it.Next(ctx) {
		it.nextPath = true
		return true
	}
	return false
}

func (it *results) Result() interface{} {
	m := make(map[string]graph.Ref)
	it.it.TagResults(m)
	if it.col == query.Raw {
		return m
	}
	out := "****\n"
	tagKeys := make([]string, len(m))
	i := 0
	for k := range m {
		tagKeys[i] = k
		i++
	}
	sort.Strings(tagKeys)
	for _, k := range tagKeys {
		if k == "$_" {
			continue
		}
		out += fmt.Sprintf("%s : %s\n", k, it.s.qs.NameOf(m[k]))
	}
	return out
}

func (it *results) Err() error {
	return it.it.Err()
}

func (it *results) Close() error {
	return it.it.Close()
}
