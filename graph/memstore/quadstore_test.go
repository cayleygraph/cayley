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
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/shape"
	"github.com/cayleygraph/cayley/writer"
	"github.com/cayleygraph/quad"
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
	qs := New()
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

func TestMemstore(t *testing.T) {
	graphtest.TestAll(t, func(t testing.TB) (graph.QuadStore, graph.Options, func()) {
		return New(), nil, func() {}
	}, &graphtest.Config{
		AlwaysRunIntegration: true,
	})
}

func BenchmarkMemstore(b *testing.B) {
	graphtest.BenchmarkAll(b, func(t testing.TB) (graph.QuadStore, graph.Options, func()) {
		return New(), nil, func() {}
	}, &graphtest.Config{
		AlwaysRunIntegration: true,
	})
}

type pair struct {
	query string
	value int64
}

func TestMemstoreValueOf(t *testing.T) {
	qs, _, index := makeTestStore(simpleGraph)
	exp := graph.Stats{
		Nodes: graph.Size{Size: 11, Exact: true},
		Quads: graph.Size{Size: 11, Exact: true},
	}
	st, err := qs.Stats(context.Background(), true)
	require.NoError(t, err)
	require.Equal(t, exp, st, "Unexpected quadstore size")

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
	ctx := context.TODO()
	qs, _, _ := makeTestStore(simpleGraph)

	fixed := iterator.NewFixed()
	fixed.Add(qs.ValueOf(quad.Raw("C")))

	fixed2 := iterator.NewFixed()
	fixed2.Add(qs.ValueOf(quad.Raw("follows")))

	all := qs.NodesAllIterator()

	const allTag = "all"
	innerAnd := iterator.NewAnd(
		iterator.NewLinksTo(qs, fixed2, quad.Predicate),
		iterator.NewLinksTo(qs, iterator.Tag(all, allTag), quad.Object),
	)

	hasa := iterator.NewHasA(qs, innerAnd, quad.Subject)
	outerAnd := iterator.NewAnd(fixed, hasa)

	if !outerAnd.Next(ctx) {
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
		m := make(map[string]graph.Ref, 1)
		outerAnd.TagResults(m)
		got = append(got, quad.ToString(qs.NameOf(m[allTag])))
		if !outerAnd.NextPath(ctx) {
			break
		}
	}
	sort.Strings(got)

	if !reflect.DeepEqual(got, expect) {
		t.Errorf("Unexpected result, got:%q expect:%q", got, expect)
	}

	if outerAnd.Next(ctx) {
		t.Error("More than one possible top level output?")
	}
}

func TestLinksToOptimization(t *testing.T) {
	qs, _, _ := makeTestStore(simpleGraph)

	lto := shape.BuildIterator(qs, shape.Quads{
		{Dir: quad.Object, Values: shape.Lookup{quad.Raw("cool")}},
	})

	newIt, changed := lto.Optimize()
	if changed {
		t.Errorf("unexpected optimization step")
	}
	if _, ok := newIt.(*Iterator); !ok {
		t.Fatal("Didn't swap out to LLRB")
	}
}

func TestRemoveQuad(t *testing.T) {
	ctx := context.TODO()
	qs, w, _ := makeTestStore(simpleGraph)

	err := w.RemoveQuad(quad.Make(
		"E",
		"follows",
		"F",
		nil,
	))

	if err != nil {
		t.Error("Couldn't remove quad", err)
	}

	fixed := iterator.NewFixed()
	fixed.Add(qs.ValueOf(quad.Raw("E")))

	fixed2 := iterator.NewFixed()
	fixed2.Add(qs.ValueOf(quad.Raw("follows")))

	innerAnd := iterator.NewAnd(
		iterator.NewLinksTo(qs, fixed, quad.Subject),
		iterator.NewLinksTo(qs, fixed2, quad.Predicate),
	)

	hasa := iterator.NewHasA(qs, innerAnd, quad.Object)

	newIt, _ := hasa.Optimize()
	if newIt.Next(ctx) {
		t.Error("E should not have any followers.")
	}
}

func TestTransaction(t *testing.T) {
	qs, w, _ := makeTestStore(simpleGraph)
	st, err := qs.Stats(context.Background(), true)
	require.NoError(t, err)

	tx := graph.NewTransaction()
	tx.AddQuad(quad.Make(
		"E",
		"follows",
		"G",
		nil))
	tx.RemoveQuad(quad.Make(
		"Non",
		"existent",
		"quad",
		nil))

	err = w.ApplyTransaction(tx)
	if err == nil {
		t.Error("Able to remove a non-existent quad")
	}
	st2, err := qs.Stats(context.Background(), true)
	require.NoError(t, err)
	require.Equal(t, st, st2, "Appended a new quad in a failed transaction")
}
