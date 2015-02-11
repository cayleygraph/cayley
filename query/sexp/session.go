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
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/query"
)

type Session struct {
	qs    graph.QuadStore
	debug bool
}

func NewSession(qs graph.QuadStore) *Session {
	var s Session
	s.qs = qs
	return &s
}

func (s *Session) Debug(ok bool) {
	s.debug = ok
}

func (s *Session) Parse(input string) (query.ParseResult, error) {
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
				return query.ParseFail, fmt.Errorf("too many close parentheses at char %d: %s", i, input[min:i])
			}
		}
	}
	if parenDepth > 0 {
		return query.ParseMore, nil
	}
	if len(ParseString(input)) > 0 {
		return query.Parsed, nil
	}
	return query.ParseFail, errors.New("invalid syntax")
}

func (s *Session) Execute(input string, out chan interface{}, limit int) {
	it := BuildIteratorTreeForQuery(s.qs, input)
	newIt, changed := it.Optimize()
	if changed {
		it = newIt
	}

	if s.debug {
		b, err := json.MarshalIndent(it.Describe(), "", "  ")
		if err != nil {
			fmt.Printf("failed to format description: %v", err)
		} else {
			fmt.Printf("%s", b)
		}
	}
	nResults := 0
	for graph.Next(it) {
		tags := make(map[string]graph.Value)
		it.TagResults(tags)
		out <- &tags
		nResults++
		if nResults > limit && limit != -1 {
			break
		}
		for it.NextPath() == true {
			tags := make(map[string]graph.Value)
			it.TagResults(tags)
			out <- &tags
			nResults++
			if nResults > limit && limit != -1 {
				break
			}
		}
	}
	close(out)
}

func (s *Session) Format(result interface{}) string {
	out := fmt.Sprintln("****")
	tags := result.(map[string]graph.Value)
	tagKeys := make([]string, len(tags))
	i := 0
	for k := range tags {
		tagKeys[i] = k
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
