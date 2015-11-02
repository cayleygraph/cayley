// Copyright 2015 The Cayley Authors. All rights reserved.
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

package iterator

import (
	"reflect"
	"sort"
	"testing"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/quad"
)

func singleHop(pred string) graph.ApplyMorphism {
	return func(qs graph.QuadStore, it graph.Iterator) graph.Iterator {
		fixed := qs.FixedIterator()
		fixed.Add(pred)
		predlto := NewLinksTo(qs, fixed, quad.Predicate)
		lto := NewLinksTo(qs, it.Clone(), quad.Subject)
		and := NewAnd(qs)
		and.AddSubIterator(lto)
		and.AddSubIterator(predlto)
		return NewHasA(qs, and, quad.Object)
	}
}

var rec_test_qs = &store{
	data: []quad.Quad{
		{"alice", "parent", "bob", ""},
		{"bob", "parent", "charlie", ""},
		{"charlie", "parent", "dani", ""},
		{"charlie", "parent", "bob", ""},
		{"dani", "parent", "emily", ""},
		{"fred", "follows", "alice", ""},
		{"greg", "follows", "alice", ""},
	},
}

func TestRecursiveNext(t *testing.T) {
	qs := rec_test_qs
	start := qs.FixedIterator()
	start.Add("alice")
	r := NewRecursive(qs, start, singleHop("parent"))
	expected := []string{"bob", "charlie", "dani", "emily"}

	var got []string
	for graph.Next(r) {
		got = append(got, qs.NameOf(r.Result()))
	}
	sort.Strings(expected)
	sort.Strings(got)
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("Failed to %s, got: %v, expected: %v", "check basic recursive iterator", got, expected)
	}
}

func TestRecursiveContains(t *testing.T) {
	qs := rec_test_qs
	start := qs.FixedIterator()
	start.Add("alice")
	r := NewRecursive(qs, start, singleHop("parent"))
	values := []string{"charlie", "bob", "not"}
	expected := []bool{true, true, false}

	for i, v := range values {
		ok := r.Contains(qs.ValueOf(v))
		if expected[i] != ok {
			t.Errorf("Failed to %s, value: %s, got: %v, expected: %v", "check basic recursive contains", v, ok, expected[i])
		}
	}
}

func TestRecursiveNextPath(t *testing.T) {
	qs := rec_test_qs
	start := qs.NodesAllIterator()
	start.Tagger().Add("person")
	it := singleHop("follows")(qs, start)
	and := NewAnd(qs)
	and.AddSubIterator(it)
	fixed := qs.FixedIterator()
	fixed.Add("alice")
	and.AddSubIterator(fixed)
	r := NewRecursive(qs, and, singleHop("parent"))

	expected := []string{"fred", "fred", "fred", "fred", "greg", "greg", "greg", "greg"}
	var got []string
	for graph.Next(r) {
		res := make(map[string]graph.Value)
		r.TagResults(res)
		got = append(got, qs.NameOf(res["person"]))
	}
	sort.Strings(expected)
	sort.Strings(got)
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("Failed to check NextPath, got: %v, expected: %v", got, expected)
	}
}
