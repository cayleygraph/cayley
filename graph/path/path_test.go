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

package path

import (
	"reflect"
	"sort"
	"testing"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/quad"

	_ "github.com/google/cayley/graph/memstore"
	_ "github.com/google/cayley/writer"
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

func runTag(path *Path, tag string) []string {
	var out []string
	it := path.BuildIterator()
	it, _ = it.Optimize()
	for graph.Next(it) {
		tags := make(map[string]graph.Value)
		it.TagResults(tags)
		out = append(out, path.qs.NameOf(tags[tag]))
		for it.NextPath() {
			tags := make(map[string]graph.Value)
			it.TagResults(tags)
			out = append(out, path.qs.NameOf(tags[tag]))
		}
	}
	return out
}

type test struct {
	message string
	path    *Path
	expect  []string
	tag     string
}

func testSet(qs graph.QuadStore) []test {
	return []test{
		{
			message: "use out",
			path:    StartPath(qs, "A").Out("follows"),
			expect:  []string{"B"},
		},
		{
			message: "use in",
			path:    StartPath(qs, "B").In("follows"),
			expect:  []string{"A", "C", "D"},
		},
		{
			message: "use path Out",
			path:    StartPath(qs, "B").Out(StartPath(qs, "predicates").Out("are")),
			expect:  []string{"F", "cool"},
		},
		{
			message: "use And",
			path: StartPath(qs, "D").Out("follows").And(
				StartPath(qs, "C").Out("follows")),
			expect: []string{"B"},
		},
		{
			message: "use Or",
			path: StartPath(qs, "F").Out("follows").Or(
				StartPath(qs, "A").Out("follows")),
			expect: []string{"B", "G"},
		},
		{
			message: "implicit All",
			path:    StartPath(qs),
			expect:  []string{"A", "B", "C", "D", "E", "F", "G", "follows", "status", "cool", "status_graph", "predicates", "are"},
		},
		{
			message: "follow",
			path:    StartPath(qs, "C").Follow(StartMorphism().Out("follows").Out("follows")),
			expect:  []string{"B", "F", "G"},
		},
		{
			message: "followR",
			path:    StartPath(qs, "F").FollowReverse(StartMorphism().Out("follows").Out("follows")),
			expect:  []string{"A", "C", "D"},
		},
		{
			message: "is, tag, instead of FollowR",
			path:    StartPath(qs).Tag("first").Follow(StartMorphism().Out("follows").Out("follows")).Is("F"),
			expect:  []string{"A", "C", "D"},
			tag:     "first",
		},
		{
			message: "use Except to filter out a single vertex",
			path:    StartPath(qs, "A", "B").Except(StartPath(qs, "A")),
			expect:  []string{"B"},
		},
		{
			message: "use chained Except",
			path:    StartPath(qs, "A", "B", "C").Except(StartPath(qs, "B")).Except(StartPath(qs, "A")),
			expect:  []string{"C"},
		},
		{
			message: "show a simple save",
			path:    StartPath(qs).Save("status", "somecool"),
			tag:     "somecool",
			expect:  []string{"cool", "cool", "cool"},
		},
		{
			message: "show a simple saveR",
			path:    StartPath(qs, "cool").SaveReverse("status", "who"),
			tag:     "who",
			expect:  []string{"G", "D", "B"},
		},
		{
			message: "show a simple Has",
			path:    StartPath(qs).Has("status", "cool"),
			expect:  []string{"G", "D", "B"},
		},
		{
			message: "show a double Has",
			path:    StartPath(qs).Has("status", "cool").Has("follows", "F"),
			expect:  []string{"B"},
		},
	}
}

func TestMorphisms(t *testing.T) {
	qs := makeTestStore(simpleGraph)
	for _, test := range testSet(qs) {
		var got []string
		if test.tag == "" {
			got = runTopLevel(test.path)
		} else {
			got = runTag(test.path, test.tag)
		}
		sort.Strings(got)
		sort.Strings(test.expect)
		if !reflect.DeepEqual(got, test.expect) {
			t.Errorf("Failed to %s, got: %v expected: %v", test.message, got, test.expect)
		}
	}
}
