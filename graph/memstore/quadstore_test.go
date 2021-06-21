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
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/cayley/query/shape"
	"github.com/cayleygraph/cayley/writer"
	"github.com/cayleygraph/quad"

	"github.com/RyouZhang/async-go"
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
		Nodes: refs.Size{Value: 11, Exact: true},
		Quads: refs.Size{Value: 11, Exact: true},
	}
	st, err := qs.Stats(context.Background(), true)
	require.NoError(t, err)
	require.Equal(t, exp, st, "Unexpected quadstore size")

	for _, test := range index {
		v, err := qs.ValueOf(quad.Raw(test.query))
		require.NoError(t, err)
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
	qsv, err := qs.ValueOf(quad.Raw("C"))
	require.NoError(t, err)
	fixed.Add(qsv)

	fixed2 := iterator.NewFixed()
	qsv, err = qs.ValueOf(quad.Raw("follows"))
	require.NoError(t, err)
	fixed2.Add(qsv)

	all := qs.NodesAllIterator()

	const allTag = "all"
	innerAnd := iterator.NewAnd(
		graph.NewLinksTo(qs, fixed2, quad.Predicate),
		graph.NewLinksTo(qs, iterator.Tag(all, allTag), quad.Object),
	)

	hasa := graph.NewHasA(qs, innerAnd, quad.Subject)
	outerAnd := iterator.NewAnd(fixed, hasa).Iterate()

	if !outerAnd.Next(ctx) {
		t.Error("Expected one matching subtree")
	}
	val := outerAnd.Result()
	vn, err := qs.NameOf(val)
	require.NoError(t, err)
	if vn != quad.Raw("C") {
		t.Errorf("Matching subtree should be %s, got %s", "barak", vn)
	}

	var (
		got    []string
		expect = []string{"B", "D"}
	)
	for {
		m := make(map[string]graph.Ref, 1)
		outerAnd.TagResults(m)
		mv, err := qs.NameOf(m[allTag])
		require.NoError(t, err)
		got = append(got, quad.ToString(mv))
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

	lto := shape.BuildIterator(context.TODO(), qs, shape.Quads{
		{Dir: quad.Object, Values: shape.Lookup{quad.Raw("cool")}},
	})

	newIt, changed := lto.Optimize(context.TODO())
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
	qsv, err := qs.ValueOf(quad.Raw("E"))
	require.NoError(t, err)
	fixed.Add(qsv)

	fixed2 := iterator.NewFixed()
	qsv, err = qs.ValueOf(quad.Raw("follows"))
	require.NoError(t, err)
	fixed2.Add(qsv)

	innerAnd := iterator.NewAnd(
		graph.NewLinksTo(qs, fixed, quad.Subject),
		graph.NewLinksTo(qs, fixed2, quad.Predicate),
	)

	hasa := graph.NewHasA(qs, innerAnd, quad.Object)

	newIt, _ := hasa.Optimize(ctx)
	if newIt.Iterate().Next(ctx) {
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

// test multi thread insert and query
func TestMultiThreadQuery(t *testing.T) {
	qs, _, _ := makeTestStore(simpleGraph)

	// we make 50 insert, 50 query
	funcs := make([]async.LambdaMethod, 100)
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			index := i
			funcs[i] = func() (interface{}, error) {
				id, flag := qs.AddQuad(quad.Make(
					fmt.Sprintf("E_%d", index), "follows", "G", nil),
				)
				if !flag {
					return nil, fmt.Errorf("quard exist:%d", id)
				}
				return id, nil
			}
		} else {
			funcs[i] = func() (interface{}, error) {
				ctx := context.Background()
				followers, err := path.StartPath(qs, quad.Raw("G")).In("follows").Iterate(ctx).AllValues(qs)
				if err != nil {
					return nil, err
				}
				return followers, nil
			}
		}
	}

	results := async.All(funcs, 1*time.Second)
	for _, result := range results {
		switch result.(type) {
		case error:
			require.NoError(t, result.(error))
		}
	}
}
