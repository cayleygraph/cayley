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

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/writer"
	"github.com/stretchr/testify/require"
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
	quad.MakeRaw("A", "follows", "B", ""),
	quad.MakeRaw("C", "follows", "B", ""),
	quad.MakeRaw("C", "follows", "D", ""),
	quad.MakeRaw("D", "follows", "B", ""),
	quad.MakeRaw("B", "follows", "F", ""),
	quad.MakeRaw("F", "follows", "G", ""),
	quad.MakeRaw("D", "follows", "G", ""),
	quad.MakeRaw("E", "follows", "F", ""),
	quad.MakeRaw("B", "status", "cool", "status_graph"),
	quad.MakeRaw("D", "status", "cool", "status_graph"),
	quad.MakeRaw("G", "status", "cool", "status_graph"),
}

func makeTestStore(data []quad.Quad) (*QuadStore, graph.QuadWriter, []pair) {
	seen := make(map[string]struct{})
	qs := newQuadStore()
	var (
		val int64
		ind []pair
	)
	writer, _ := writer.NewSingleReplication(qs, nil)
	for _, t := range data {
		for _, dir := range quad.Directions {
			qp := t.GetString(dir)
			if _, ok := seen[qp]; !ok && qp != "" {
				val++
				ind = append(ind, pair{qp, val})
				seen[qp] = struct{}{}
			}
		}

		writer.AddQuad(t)
		val++
	}
	return qs, writer, ind
}

func TestMemstoreAll(t *testing.T) {
	graphtest.TestAll(t, func(t testing.TB) (graph.QuadStore, graph.Options, func()) {
		return newQuadStore(), nil, func() {}
	}, &graphtest.Config{
		SkipNodeDelAfterQuadDel: true,
	})
}

type pair struct {
	query string
	value int64
}

func TestMemstore(t *testing.T) {
	qs, _, index := makeTestStore(simpleGraph)
	require.Equal(t, int64(22), qs.Size())

	for _, test := range index {
		v := qs.ValueOf(quad.Raw(test.query))
		switch v := v.(type) {
		default:
			t.Errorf("ValueOf(%q) returned unexpected type, got:%T expected int64", test.query, v)
		case bnode:
			require.Equal(t, test.value, int64(v))
		}
	}
}

func TestIteratorsAndNextResultOrderA(t *testing.T) {
	qs, _, _ := makeTestStore(simpleGraph)

	fixed := qs.FixedIterator()
	fixed.Add(qs.ValueOf(quad.Raw("C")))

	fixed2 := qs.FixedIterator()
	fixed2.Add(qs.ValueOf(quad.Raw("follows")))

	all := qs.NodesAllIterator()

	innerAnd := iterator.NewAnd(qs,
		iterator.NewLinksTo(qs, fixed2, quad.Predicate),
		iterator.NewLinksTo(qs, all, quad.Object),
	)

	hasa := iterator.NewHasA(qs, innerAnd, quad.Subject)
	outerAnd := iterator.NewAnd(qs, fixed, hasa)

	if !outerAnd.Next() {
		t.Error("Expected one matching subtree")
	}
	val := outerAnd.Result()
	if qs.NameOf(val) != quad.Raw("C") {
		t.Errorf("Matching subtree should be %s, got %s", "barak", qs.NameOf(val))
	}

	var (
		got    []string
		expect = []string{"B", "D"}
	)
	for {
		got = append(got, quad.StringOf(qs.NameOf(all.Result())))
		if !outerAnd.NextPath() {
			break
		}
	}
	sort.Strings(got)

	if !reflect.DeepEqual(got, expect) {
		t.Errorf("Unexpected result, got:%q expect:%q", got, expect)
	}

	if outerAnd.Next() {
		t.Error("More than one possible top level output?")
	}
}

func TestLinksToOptimization(t *testing.T) {
	qs, _, _ := makeTestStore(simpleGraph)

	fixed := qs.FixedIterator()
	fixed.Add(qs.ValueOf(quad.Raw("cool")))

	lto := iterator.NewLinksTo(qs, fixed, quad.Object)
	lto.Tagger().Add("foo")

	newIt, changed := lto.Optimize()
	if !changed {
		t.Error("Iterator didn't change")
	}
	if _, ok := newIt.(*Iterator); !ok {
		t.Fatal("Didn't swap out to LLRB")
	}

	v := newIt.(*Iterator)
	vClone := v.Clone()
	origDesc := graph.DescribeIterator(v)
	cloneDesc := graph.DescribeIterator(vClone)
	origDesc.UID, cloneDesc.UID = 0, 0 // We are more strict now, so fake UID equality.
	if !reflect.DeepEqual(cloneDesc, origDesc) {
		t.Fatalf("Unexpected iterator description.\ngot: %#v\nexpect: %#v", cloneDesc, origDesc)
	}
	vt := vClone.Tagger()
	if len(vt.Tags()) < 1 || vt.Tags()[0] != "foo" {
		t.Fatal("Tag on LinksTo did not persist")
	}
}

func TestRemoveQuad(t *testing.T) {
	qs, w, _ := makeTestStore(simpleGraph)

	err := w.RemoveQuad(quad.MakeRaw(
		"E",
		"follows",
		"F",
		"",
	))

	if err != nil {
		t.Error("Couldn't remove quad", err)
	}

	fixed := qs.FixedIterator()
	fixed.Add(qs.ValueOf(quad.Raw("E")))

	fixed2 := qs.FixedIterator()
	fixed2.Add(qs.ValueOf(quad.Raw("follows")))

	innerAnd := iterator.NewAnd(qs,
		iterator.NewLinksTo(qs, fixed, quad.Subject),
		iterator.NewLinksTo(qs, fixed2, quad.Predicate),
	)

	hasa := iterator.NewHasA(qs, innerAnd, quad.Object)

	newIt, _ := hasa.Optimize()
	if newIt.Next() {
		t.Error("E should not have any followers.")
	}
}

func TestTransaction(t *testing.T) {
	qs, w, _ := makeTestStore(simpleGraph)
	size := qs.Size()

	tx := graph.NewTransaction()
	tx.AddQuad(quad.MakeRaw(
		"E",
		"follows",
		"G",
		""))
	tx.RemoveQuad(quad.MakeRaw(
		"Non",
		"existent",
		"quad",
		""))

	err := w.ApplyTransaction(tx)
	if err == nil {
		t.Error("Able to remove a non-existent quad")
	}
	if size != qs.Size() {
		t.Error("Appended a new quad in a failed transaction")
	}
}
