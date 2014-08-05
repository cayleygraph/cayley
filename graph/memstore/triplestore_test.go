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

package memstore

import (
	"reflect"
	"sort"
	"testing"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

// This is a simple test graph.
//
//    +---+                        +---+
//    | A |-------               ->| F |<--
//    +---+       \------>+---+-/  +---+   \--+---+
//                 ------>|#B#|      |        | E |
//    +---+-------/      >+---+      |        +---+
//    | C |             /            v
//    +---+           -/           +---+
//      ----    +---+/             |#G#|
//          \-->|#D#|------------->+---+
//              +---+
//
var simpleGraph = []quad.Quad{
	{"A", "follows", "B", ""},
	{"C", "follows", "B", ""},
	{"C", "follows", "D", ""},
	{"D", "follows", "B", ""},
	{"B", "follows", "F", ""},
	{"F", "follows", "G", ""},
	{"D", "follows", "G", ""},
	{"E", "follows", "F", ""},
	{"B", "status", "cool", "status_graph"},
	{"D", "status", "cool", "status_graph"},
	{"G", "status", "cool", "status_graph"},
}

func makeTestStore(data []quad.Quad) (*TripleStore, []pair) {
	seen := make(map[string]struct{})
	ts := newTripleStore()
	var (
		val int64
		ind []pair
	)
	for _, t := range data {
		for _, qp := range []string{t.Subject, t.Predicate, t.Object, t.Label} {
			if _, ok := seen[qp]; !ok && qp != "" {
				val++
				ind = append(ind, pair{qp, val})
				seen[qp] = struct{}{}
			}
		}
		ts.AddTriple(t)
	}
	return ts, ind
}

type pair struct {
	query string
	value int64
}

func TestMemstore(t *testing.T) {
	ts, index := makeTestStore(simpleGraph)
	if size := ts.Size(); size != int64(len(simpleGraph)) {
		t.Errorf("Triple store has unexpected size, got:%d expected %d", size, len(simpleGraph))
	}
	for _, test := range index {
		v := ts.ValueOf(test.query)
		switch v := v.(type) {
		default:
			t.Errorf("ValueOf(%q) returned unexpected type, got:%T expected int64", test.query, v)
		case int64:
			if v != test.value {
				t.Errorf("ValueOf(%q) returned unexpected value, got:%d expected:%d", test.query, v, test.value)
			}
		}
	}
}

func TestIteratorsAndNextResultOrderA(t *testing.T) {
	ts, _ := makeTestStore(simpleGraph)

	fixed := ts.FixedIterator()
	fixed.Add(ts.ValueOf("C"))

	fixed2 := ts.FixedIterator()
	fixed2.Add(ts.ValueOf("follows"))

	all := ts.NodesAllIterator()

	innerAnd := iterator.NewAnd()
	innerAnd.AddSubIterator(iterator.NewLinksTo(ts, fixed2, quad.Predicate))
	innerAnd.AddSubIterator(iterator.NewLinksTo(ts, all, quad.Object))

	hasa := iterator.NewHasA(ts, innerAnd, quad.Subject)
	outerAnd := iterator.NewAnd()
	outerAnd.AddSubIterator(fixed)
	outerAnd.AddSubIterator(hasa)

	val, ok := outerAnd.Next()
	if !ok {
		t.Error("Expected one matching subtree")
	}
	if ts.NameOf(val) != "C" {
		t.Errorf("Matching subtree should be %s, got %s", "barak", ts.NameOf(val))
	}

	var (
		got    []string
		expect = []string{"B", "D"}
	)
	for {
		got = append(got, ts.NameOf(all.Result()))
		if !outerAnd.NextResult() {
			break
		}
	}
	sort.Strings(got)

	if !reflect.DeepEqual(got, expect) {
		t.Errorf("Unexpected result, got:%q expect:%q", got, expect)
	}

	val, ok = outerAnd.Next()
	if ok {
		t.Error("More than one possible top level output?")
	}
}

func TestLinksToOptimization(t *testing.T) {
	ts, _ := makeTestStore(simpleGraph)

	fixed := ts.FixedIterator()
	fixed.Add(ts.ValueOf("cool"))

	lto := iterator.NewLinksTo(ts, fixed, quad.Object)
	lto.Tagger().Add("foo")

	newIt, changed := lto.Optimize()
	if !changed {
		t.Error("Iterator didn't change")
	}
	if newIt.Type() != Type() {
		t.Fatal("Didn't swap out to LLRB")
	}

	v := newIt.(*Iterator)
	v_clone := v.Clone()
	if v_clone.DebugString(0) != v.DebugString(0) {
		t.Fatal("Wrong iterator. Got ", v_clone.DebugString(0))
	}
	vt := v_clone.Tagger()
	if len(vt.Tags()) < 1 || vt.Tags()[0] != "foo" {
		t.Fatal("Tag on LinksTo did not persist")
	}
}

func TestRemoveTriple(t *testing.T) {
	ts, _ := makeTestStore(simpleGraph)

	ts.RemoveTriple(quad.Quad{"E", "follows", "F", ""})

	fixed := ts.FixedIterator()
	fixed.Add(ts.ValueOf("E"))

	fixed2 := ts.FixedIterator()
	fixed2.Add(ts.ValueOf("follows"))

	innerAnd := iterator.NewAnd()
	innerAnd.AddSubIterator(iterator.NewLinksTo(ts, fixed, quad.Subject))
	innerAnd.AddSubIterator(iterator.NewLinksTo(ts, fixed2, quad.Predicate))

	hasa := iterator.NewHasA(ts, innerAnd, quad.Object)

	newIt, _ := hasa.Optimize()
	_, ok := graph.Next(newIt)
	if ok {
		t.Error("E should not have any followers.")
	}
}
