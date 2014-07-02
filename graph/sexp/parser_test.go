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

	. "github.com/smartystreets/goconvey/convey"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/memstore"
)

func TestBadParse(t *testing.T) {
	str := ParseString("()")
	if str != "" {
		t.Errorf("It parsed! Got \"%s\"", str)
	}
}

func TestParseSexpWithMemstore(t *testing.T) {
	Convey("With a Memstore", t, func() {
		ts := memstore.NewTripleStore()

		Convey("It should parse an empty query", func() {
			it := BuildIteratorTreeForQuery(ts, "()")
			So(it.Type(), ShouldEqual, "null")
		})

		Convey("It should get a single triple linkage", func() {
			ts.AddTriple(&graph.Triple{"i", "can", "win", ""})
			query := "($a (:can \"win\"))"
			So(len(query), ShouldEqual, 17)
			it := BuildIteratorTreeForQuery(ts, query)
			So(it.Type(), ShouldEqual, "and")
			out, ok := it.Next()
			So(ok, ShouldBeTrue)
			So(out, ShouldEqual, ts.ValueOf("i"))
		})

		Convey("It can get an internal linkage", func() {
			ts.AddTriple(&graph.Triple{"i", "can", "win", ""})
			query := "(\"i\" (:can $a))"
			it := BuildIteratorTreeForQuery(ts, query)
			So(it.Type(), ShouldEqual, "and")
			out, ok := it.Next()
			So(ok, ShouldBeTrue)
			So(out, ShouldEqual, ts.ValueOf("i"))
		})

	})
}

func TestTreeConstraintParse(t *testing.T) {
	ts := memstore.NewTripleStore()
	ts.AddTriple(&graph.Triple{"i", "like", "food", ""})
	ts.AddTriple(&graph.Triple{"food", "is", "good", ""})
	query := "(\"i\"\n" +
		"(:like\n" +
		"($a (:is :good))))"
	it := BuildIteratorTreeForQuery(ts, query)
	if it.Type() != "and" {
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
	ts := memstore.NewTripleStore()
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
	ts := memstore.NewTripleStore()
	ts.AddTriple(&graph.Triple{"i", "like", "food", ""})
	ts.AddTriple(&graph.Triple{"i", "like", "beer", ""})
	ts.AddTriple(&graph.Triple{"you", "like", "beer", ""})
	query := "($a \n" +
		"(:like :beer)\n" +
		"(:like \"food\"))"
	it := BuildIteratorTreeForQuery(ts, query)
	if it.Type() != "and" {
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
