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
	"github.com/google/cayley/quad"

	_ "github.com/google/cayley/graph/memstore"
	_ "github.com/google/cayley/writer"
)

func TestBadParse(t *testing.T) {
	str := ParseString("()")
	if str != "" {
		t.Errorf("Unexpected parse result, got:%q", str)
	}
}

var testQueries = []struct {
	message string
	add     quad.Quad
	query   string
	typ     graph.Type
	expect  string
}{
	{
		message: "get a single quad linkage",
		add:     quad.Quad{"i", "can", "win", ""},
		query:   "($a (:can \"win\"))",
		typ:     graph.And,
		expect:  "i",
	},
	{
		message: "get a single quad linkage",
		add:     quad.Quad{"i", "can", "win", ""},
		query:   "(\"i\" (:can $a))",
		typ:     graph.And,
		expect:  "i",
	},
}

func TestMemstoreBackedSexp(t *testing.T) {
	qs, _ := graph.NewQuadStore("memstore", "", nil)
	w, _ := graph.NewQuadWriter("single", qs, nil)
	emptyIt := BuildIteratorTreeForQuery(qs, "()")
	if emptyIt.Type() != graph.Null {
		t.Errorf(`Incorrect type for empty query, got:%q expect: "null"`, emptyIt.Type())
	}
	for _, test := range testQueries {
		if test.add.IsValid() {
			w.AddQuad(test.add)
		}
		it := BuildIteratorTreeForQuery(qs, test.query)
		if it.Type() != test.typ {
			t.Errorf("Incorrect type for %s, got:%q expect %q", test.message, it.Type(), test.expect)
		}
		if !graph.Next(it) {
			t.Errorf("Failed to %s", test.message)
		}
		got := it.Result()
		if expect := qs.ValueOf(test.expect); got != expect {
			t.Errorf("Incorrect result for %s, got:%v expect %v", test.message, got, expect)
		}
	}
}

func TestTreeConstraintParse(t *testing.T) {
	qs, _ := graph.NewQuadStore("memstore", "", nil)
	w, _ := graph.NewQuadWriter("single", qs, nil)
	w.AddQuad(quad.Quad{"i", "like", "food", ""})
	w.AddQuad(quad.Quad{"food", "is", "good", ""})
	query := "(\"i\"\n" +
		"(:like\n" +
		"($a (:is :good))))"
	it := BuildIteratorTreeForQuery(qs, query)
	if it.Type() != graph.And {
		t.Errorf("Odd iterator tree. Got: %#v", it.Describe())
	}
	if !graph.Next(it) {
		t.Error("Got no results")
	}
	out := it.Result()
	if out != qs.ValueOf("i") {
		t.Errorf("Got %d, expected %d", out, qs.ValueOf("i"))
	}
}

func TestTreeConstraintTagParse(t *testing.T) {
	qs, _ := graph.NewQuadStore("memstore", "", nil)
	w, _ := graph.NewQuadWriter("single", qs, nil)
	w.AddQuad(quad.Quad{"i", "like", "food", ""})
	w.AddQuad(quad.Quad{"food", "is", "good", ""})
	query := "(\"i\"\n" +
		"(:like\n" +
		"($a (:is :good))))"
	it := BuildIteratorTreeForQuery(qs, query)
	if !graph.Next(it) {
		t.Error("Got no results")
	}
	tags := make(map[string]graph.Value)
	it.TagResults(tags)
	if qs.NameOf(tags["$a"]) != "food" {
		t.Errorf("Got %s, expected food", qs.NameOf(tags["$a"]))
	}

}

func TestMultipleConstraintParse(t *testing.T) {
	qs, _ := graph.NewQuadStore("memstore", "", nil)
	w, _ := graph.NewQuadWriter("single", qs, nil)
	for _, tv := range []quad.Quad{
		{"i", "like", "food", ""},
		{"i", "like", "beer", ""},
		{"you", "like", "beer", ""},
	} {
		w.AddQuad(tv)
	}
	query := `(
		$a
		(:like :beer)
		(:like "food")
	)`
	it := BuildIteratorTreeForQuery(qs, query)
	if it.Type() != graph.And {
		t.Errorf("Odd iterator tree. Got: %#v", it.Describe())
	}
	if !graph.Next(it) {
		t.Error("Got no results")
	}
	out := it.Result()
	if out != qs.ValueOf("i") {
		t.Errorf("Got %d, expected %d", out, qs.ValueOf("i"))
	}
	if graph.Next(it) {
		t.Error("Too many results")
	}
}
