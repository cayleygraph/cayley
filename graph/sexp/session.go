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
	"errors"
	"fmt"
	"sort"

	"github.com/google/cayley/graph"
)

type Session struct {
	ts    graph.TripleStore
	debug bool
}

func NewSession(inputTripleStore graph.TripleStore) *Session {
	var s Session
	s.ts = inputTripleStore
	return &s
}

func (s *Session) ToggleDebug() {
	s.debug = !s.debug
}

func (s *Session) InputParses(input string) (graph.ParseResult, error) {
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
				return graph.ParseFail, errors.New(fmt.Sprintf("Too many close parens at char %d: %s", i, input[min:i]))
			}
		}
	}
	if parenDepth > 0 {
		return graph.ParseMore, nil
	}
	if len(ParseString(input)) > 0 {
		return graph.Parsed, nil
	}
	return graph.ParseFail, errors.New("Invalid Syntax")
}

func (s *Session) ExecInput(input string, out chan interface{}, limit int) {
	it := BuildIteratorTreeForQuery(s.ts, input)
	newIt, changed := it.Optimize()
	if changed {
		it = newIt
	}

	if s.debug {
		fmt.Println(it.DebugString(0))
	}
	nResults := 0
	for {
		_, ok := it.Next()
		if !ok {
			break
		}
		tags := make(map[string]graph.TSVal)
		it.TagResults(&tags)
		out <- &tags
		nResults++
		if nResults > limit && limit != -1 {
			break
		}
		for it.NextResult() == true {
			tags := make(map[string]graph.TSVal)
			it.TagResults(&tags)
			out <- &tags
			nResults++
			if nResults > limit && limit != -1 {
				break
			}
		}
	}
	close(out)
}

func (s *Session) ToText(result interface{}) string {
	out := fmt.Sprintln("****")
	tags := result.(*map[string]graph.TSVal)
	tagKeys := make([]string, len(*tags))
	i := 0
	for k, _ := range *tags {
		tagKeys[i] = k
		i++
	}
	sort.Strings(tagKeys)
	for _, k := range tagKeys {
		if k == "$_" {
			continue
		}
		out += fmt.Sprintf("%s : %s\n", k, s.ts.GetNameFor((*tags)[k]))
	}
	return out
}
