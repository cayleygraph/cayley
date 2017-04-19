// Copyright 2014 The Cayley Authors. All rights reserved.  //
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

// +build appengine appenginevm

package gaedatastore

import (
	"errors"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/writer"
	"github.com/stretchr/testify/require"

	"google.golang.org/appengine/aetest"
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
var simpleGraph = graphtest.MakeQuadSet()
var simpleGraphUpdate = []quad.Quad{
	quad.MakeRaw("A", "follows", "B", ""),
	quad.MakeRaw("F", "follows", "B", ""),
	quad.MakeRaw("C", "follows", "D", ""),
	quad.MakeRaw("X", "follows", "B", ""),
}

type pair struct {
	query string
	value int64
}

func makeTestStore(data []quad.Quad, opts graph.Options) (graph.QuadStore, graph.QuadWriter, []pair) {
	seen := make(map[quad.Value]struct{})

	qs, _ := newQuadStore("", opts)
	qs, _ = newQuadStoreForRequest(qs, opts)
	var (
		val int64
		ind []pair
	)
	writer, _ := writer.NewSingleReplication(qs, nil)
	for _, t := range data {
		for _, qp := range []quad.Value{t.Subject, t.Predicate, t.Object, t.Label} {
			if _, ok := seen[qp]; !ok && qp != nil {
				val++
				ind = append(ind, pair{qp.String(), val})
				seen[qp] = struct{}{}
			}
		}
	}
	writer.AddQuadSet(data)
	return qs, writer, ind
}

func createInstance() (aetest.Instance, graph.Options, error) {
	inst, err := aetest.NewInstance(&aetest.Options{"", true})
	if err != nil {
		return nil, nil, errors.New("Creation of new instance failed")
	}
	req1, err := inst.NewRequest("POST", "/api/v1/write", nil)
	if err != nil {
		return nil, nil, errors.New("Creation of new request failed")
	}
	opts := make(graph.Options)
	opts["HTTPRequest"] = req1
	return inst, opts, nil
}

func makeGAE(t testing.TB) (graph.QuadStore, graph.Options, func()) {
	inst, opts, err := createInstance()
	require.NoError(t, err)
	qs, err := newQuadStore("", opts)
	if err != nil {
		inst.Close()
		t.Fatal(err)
	}
	qs, err = newQuadStoreForRequest(qs, opts)
	if err != nil {
		inst.Close()
		t.Fatal(err)
	}
	return qs, opts, func() {
		qs.Close()
		inst.Close()
	}
}

func TestGAEAll(t *testing.T) {
	graphtest.TestAll(t, makeGAE, &graphtest.Config{
		SkipIntHorizon: true,
		UnTyped:        true,
	})
}

func TestIterators(t *testing.T) {
	qs, opts, closer := makeGAE(t)
	defer closer()

	graphtest.MakeWriter(t, qs, opts, graphtest.MakeQuadSet()...)

	require.Equal(t, int64(11), qs.Size(), "Incorrect number of quads")

	var expected = []quad.Quad{
		quad.MakeRaw("C", "follows", "B", ""),
		quad.MakeRaw("C", "follows", "D", ""),
	}

	it := qs.QuadIterator(quad.Subject, qs.ValueOf(quad.Raw("C")))
	graphtest.ExpectIteratedQuads(t, qs, it, expected, false)

	// Test contains
	it = qs.QuadIterator(quad.Label, qs.ValueOf(quad.Raw("status_graph")))
	gqs := qs.(*QuadStore)
	key := gqs.createKeyForQuad(quad.MakeRaw("G", "status", "cool", "status_graph"))
	token := &Token{quadKind, key.StringID()}

	require.True(t, it.Contains(token), "Contains failed")

	// Test cloning an iterator
	var it2 graph.Iterator
	it2 = it.Clone()
	x := it2.Describe()
	y := it.Describe()

	require.Equal(t, y.Name, x.Name, "Iterator Clone was not successful")
}
