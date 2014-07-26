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

import (
	"testing"

	"github.com/google/cayley/graph"
	_ "github.com/google/cayley/graph/memstore"
)

func TestBadParse(t *testing.T) {
	str := ParseString("()")
	if str != "" {
		t.Errorf("Unexpected parse result, got:%q", str)
	}
}

var testQueries = []struct {
	message string
	add     *graph.Triple
	query   string
	typ     graph.Type
	expect  string
}{
	{
		message: "get a single triple linkage",
		add:     &graph.Triple{"i", "can", "win", ""},
		query:   "($a (:can \"win\"))",
		typ:     graph.And,
		expect:  "i",
	},
	{
		message: "get a single triple linkage",
		add:     &graph.Triple{"i", "can", "win", ""},
		query:   "(\"i\" (:can $a))",
		typ:     graph.And,
		expect:  "i",
	},
}

func TestMemstoreBackedSexp(t *testing.T) {
	ts, _ := graph.NewTripleStore("memstore", "", nil)
	it := BuildIteratorTreeForQuery(ts, "()")
	if it.Type() != graph.Null {
		t.Errorf(`Incorrect type for empty query, got:%q expect: "null"`, it.Type())
	}
	for _, test := range testQueries {
		if test.add != nil {
			ts.AddTriple(test.add)
		}
		it := BuildIteratorTreeForQuery(ts, test.query)
		if it.Type() != test.typ {
			t.Errorf("Incorrect type for %s, got:%q expect %q", test.message, it.Type(), test.expect)
		}
		got, ok := it.Next()
		if !ok {
			t.Errorf("Failed to %s", test.message)
		}
		if expect := ts.ValueOf(test.expect); got != expect {
			t.Errorf("Incorrect result for %s, got:%v expect %v", test.message, got, expect)
		}
	}
}

func TestTreeConstraintParse(t *testing.T) {
	ts, _ := graph.NewTripleStore("memstore", "", nil)
	ts.AddTriple(&graph.Triple{"i", "like", "food", ""})
	ts.AddTriple(&graph.Triple{"food", "is", "good", ""})
	query := "(\"i\"\n" +
		"(:like\n" +
		"($a (:is :good))))"
	it := BuildIteratorTreeForQuery(ts, query)
	if it.Type() != graph.And {
		t.Error("Odd iterator tree. Got: %s", it.DebugString(0))
	}
	out, ok := it.Next()
	if !ok {
		t.Error("Got no results")
	}
	if out != ts.ValueOf("i") {
		t.Errorf("Got %d, expected %d", out, ts.ValueOf("i"))
	}
}

func TestTreeConstraintTagParse(t *testing.T) {
	ts, _ := graph.NewTripleStore("memstore", "", nil)
	ts.AddTriple(&graph.Triple{"i", "like", "food", ""})
	ts.AddTriple(&graph.Triple{"food", "is", "good", ""})
	query := "(\"i\"\n" +
		"(:like\n" +
		"($a (:is :good))))"
	it := BuildIteratorTreeForQuery(ts, query)
	_, ok := it.Next()
	if !ok {
		t.Error("Got no results")
	}
	tags := make(map[string]graph.Value)
	it.TagResults(tags)
	if ts.NameOf(tags["$a"]) != "food" {
		t.Errorf("Got %s, expected food", ts.NameOf(tags["$a"]))
	}

}

func TestMultipleConstraintParse(t *testing.T) {
	ts, _ := graph.NewTripleStore("memstore", "", nil)
	for _, tv := range []*graph.Triple{
		{"i", "like", "food", ""},
		{"i", "like", "beer", ""},
		{"you", "like", "beer", ""},
	} {
		ts.AddTriple(tv)
	}
	query := `(
		$a
		(:like :beer)
		(:like "food")
	)`
	it := BuildIteratorTreeForQuery(ts, query)
	if it.Type() != graph.And {
		t.Error("Odd iterator tree. Got: %s", it.DebugString(0))
	}
	out, ok := it.Next()
	if !ok {
		t.Error("Got no results")
	}
	if out != ts.ValueOf("i") {
		t.Errorf("Got %d, expected %d", out, ts.ValueOf("i"))
	}
	_, ok = it.Next()
	if ok {
		t.Error("Too many results")
	}
}
