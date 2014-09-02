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

package api

import (
	"reflect"
	"sort"
	"testing"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/quad"

	_ "github.com/google/cayley/graph/memstore"
	_ "github.com/google/cayley/writer"
)

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
	{"predicates", "are", "follows", ""},
	{"predicates", "are", "status", ""},
}

func makeTestStore(data []quad.Quad) graph.QuadStore {
	qs, _ := graph.NewQuadStore("memstore", "", nil)
	w, _ := graph.NewQuadWriter("single", qs, nil)
	for _, t := range data {
		w.AddQuad(t)
	}
	return qs
}

func runTopLevel(path *Path) []string {
	var out []string
	it := path.BuildIterator()
	it, _ = it.Optimize()
	for graph.Next(it) {
		v := path.qs.NameOf(it.Result())
		out = append(out, v)
	}
	return out
}

type test struct {
	message string
	path    *Path
	expect  []string
}

func testSet(qs graph.QuadStore) []test {
	return []test{
		{
			message: "use out",
			path:    V(qs, "A").Out("follows"),
			expect:  []string{"B"},
		},
		{
			message: "use in",
			path:    V(qs, "B").In("follows"),
			expect:  []string{"A", "C", "D"},
		},
		{
			message: "use path Out",
			path:    V(qs, "B").Out(V(qs, "predicates").Out("are")),
			expect:  []string{"F", "cool"},
		},
		{
			message: "in",
			path: V(qs, "D").Out("follows").And(
				V(qs, "C").Out("follows")),
			expect: []string{"B"},
		},
	}
}

func TestMorphisms(t *testing.T) {
	qs := makeTestStore(simpleGraph)
	for _, test := range testSet(qs) {
		got := runTopLevel(test.path)
		sort.Strings(got)
		sort.Strings(test.expect)
		if !reflect.DeepEqual(got, test.expect) {
			t.Errorf("Failed to %s, got: %v expected: %v", test.message, got, test.expect)
		}
	}
}
