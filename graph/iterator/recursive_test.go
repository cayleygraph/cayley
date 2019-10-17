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

package iterator_test

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphmock"
	. "github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/quad"
)

func singleHop(qs graph.QuadIndexer, pred string) Morphism {
	return func(it Shape) Shape {
		fixed := NewFixed()
		fixed.Add(refs.PreFetched(quad.Raw(pred)))
		predlto := graph.NewLinksTo(qs, fixed, quad.Predicate)
		lto := graph.NewLinksTo(qs, it, quad.Subject)
		and := NewAnd()
		and.AddSubIterator(lto)
		and.AddSubIterator(predlto)
		return graph.NewHasA(qs, and, quad.Object)
	}
}

var recTestQs = &graphmock.Store{
	Data: []quad.Quad{
		quad.MakeRaw("alice", "parent", "bob", ""),
		quad.MakeRaw("bob", "parent", "charlie", ""),
		quad.MakeRaw("charlie", "parent", "dani", ""),
		quad.MakeRaw("charlie", "parent", "bob", ""),
		quad.MakeRaw("dani", "parent", "emily", ""),
		quad.MakeRaw("fred", "follows", "alice", ""),
		quad.MakeRaw("greg", "follows", "alice", ""),
	},
}

func TestRecursiveNext(t *testing.T) {
	ctx := context.TODO()
	qs := recTestQs
	start := NewFixed()
	start.Add(refs.PreFetched(quad.Raw("alice")))
	r := NewRecursive(start, singleHop(qs, "parent"), 0).Iterate()

	expected := []string{"bob", "charlie", "dani", "emily"}
	var got []string
	for r.Next(ctx) {
		got = append(got, quad.ToString(qs.NameOf(r.Result())))
	}
	sort.Strings(expected)
	sort.Strings(got)
	require.Equal(t, expected, got)
}

func TestRecursiveContains(t *testing.T) {
	ctx := context.TODO()
	qs := recTestQs
	start := NewFixed()
	start.Add(refs.PreFetched(quad.Raw("alice")))
	r := NewRecursive(start, singleHop(qs, "parent"), 0).Lookup()
	values := []string{"charlie", "bob", "not"}
	expected := []bool{true, true, false}

	for i, v := range values {
		ok := r.Contains(ctx, qs.ValueOf(quad.Raw(v)))
		require.Equal(t, expected[i], ok)
	}
}

func TestRecursiveNextPath(t *testing.T) {
	ctx := context.TODO()
	qs := recTestQs
	start := qs.NodesAllIterator()
	start = Tag(start, "person")
	it := singleHop(qs, "follows")(start)
	and := NewAnd()
	and.AddSubIterator(it)
	fixed := NewFixed()
	fixed.Add(refs.PreFetched(quad.Raw("alice")))
	and.AddSubIterator(fixed)
	r := NewRecursive(and, singleHop(qs, "parent"), 0).Iterate()

	expected := []string{"fred", "fred", "fred", "fred", "greg", "greg", "greg", "greg"}
	var got []string
	for r.Next(ctx) {
		res := make(map[string]refs.Ref)
		r.TagResults(res)
		got = append(got, quad.ToString(qs.NameOf(res["person"])))
		for r.NextPath(ctx) {
			res := make(map[string]refs.Ref)
			r.TagResults(res)
			got = append(got, quad.ToString(qs.NameOf(res["person"])))
		}
	}
	sort.Strings(expected)
	sort.Strings(got)
	require.Equal(t, expected, got)
}
