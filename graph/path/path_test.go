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
	"io"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/quad"
	"github.com/google/cayley/quad/cquads"

	_ "github.com/google/cayley/graph/memstore"
	_ "github.com/google/cayley/writer"
)

// This is a simple test graph.
//
//  +-------+                        +------+
//  | alice |-----                 ->| fred |<--
//  +-------+     \---->+-------+-/  +------+   \-+-------+
//                ----->| #bob# |       |         | emily |
//  +---------+--/  --->+-------+       |         +-------+
//  | charlie |    /                    v
//  +---------+   /                  +--------+
//    \---    +--------+             | #greg# |
//        \-->| #dani# |------------>+--------+
//            +--------+

func loadGraph(path string, t testing.TB) []quad.Quad {
	var r io.Reader
	var simpleGraph []quad.Quad
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Failed to open %q: %v", path, err)
	}
	defer f.Close()
	r = f

	dec := cquads.NewDecoder(r)
	q1, err := dec.Unmarshal()
	if err != nil {
		t.Fatalf("Failed to Unmarshal: %v", err)
	}
	for ; err == nil; q1, err = dec.Unmarshal() {
		simpleGraph = append(simpleGraph, q1)
	}
	return simpleGraph
}

func makeTestStore(t testing.TB) graph.QuadStore {
	simpleGraph := loadGraph("../../data/testdata.nq", t)
	qs, _ := graph.NewQuadStore("memstore", "", nil)
	w, _ := graph.NewQuadWriter("single", qs, nil)
	for _, t := range simpleGraph {
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

// Define morphisms without a QuadStore

var (
	grandfollows = StartMorphism().Out("follows").Out("follows")
)

func testSet(qs graph.QuadStore) []test {
	return []test{
		{
			message: "use out",
			path:    StartPath(qs, "alice").Out("follows"),
			expect:  []string{"bob"},
		},
		{
			message: "use in",
			path:    StartPath(qs, "bob").In("follows"),
			expect:  []string{"alice", "charlie", "dani"},
		},
		{
			message: "use path Out",
			path:    StartPath(qs, "bob").Out(StartPath(qs, "predicates").Out("are")),
			expect:  []string{"fred", "cool_person"},
		},
		{
			message: "use And",
			path: StartPath(qs, "dani").Out("follows").And(
				StartPath(qs, "charlie").Out("follows")),
			expect: []string{"bob"},
		},
		{
			message: "use Or",
			path: StartPath(qs, "fred").Out("follows").Or(
				StartPath(qs, "alice").Out("follows")),
			expect: []string{"bob", "greg"},
		},
		{
			message: "implicit All",
			path:    StartPath(qs),
			expect:  []string{"alice", "bob", "charlie", "dani", "emily", "fred", "greg", "follows", "status", "cool_person", "predicates", "are"},
		},
		{
			message: "follow",
			path:    StartPath(qs, "charlie").Follow(StartMorphism().Out("follows").Out("follows")),
			expect:  []string{"bob", "fred", "greg"},
		},
		{
			message: "followR",
			path:    StartPath(qs, "fred").FollowReverse(StartMorphism().Out("follows").Out("follows")),
			expect:  []string{"alice", "charlie", "dani"},
		},
		{
			message: "is, tag, instead of FollowR",
			path:    StartPath(qs).Tag("first").Follow(StartMorphism().Out("follows").Out("follows")).Is("fred"),
			expect:  []string{"alice", "charlie", "dani"},
			tag:     "first",
		},
		{
			message: "use Except to filter out a single vertex",
			path:    StartPath(qs, "alice", "bob").Except(StartPath(qs, "alice")),
			expect:  []string{"bob"},
		},
		{
			message: "use chained Except",
			path:    StartPath(qs, "alice", "bob", "charlie").Except(StartPath(qs, "bob")).Except(StartPath(qs, "alice")),
			expect:  []string{"charlie"},
		},
		{
			message: "show a simple save",
			path:    StartPath(qs).Save("status", "somecool"),
			tag:     "somecool",
			expect:  []string{"cool_person", "cool_person", "cool_person"},
		},
		{
			message: "show a simple saveR",
			path:    StartPath(qs, "cool_person").SaveReverse("status", "who"),
			tag:     "who",
			expect:  []string{"greg", "dani", "bob"},
		},
		{
			message: "show a simple Has",
			path:    StartPath(qs).Has("status", "cool_person"),
			expect:  []string{"greg", "dani", "bob"},
		},
		{
			message: "show a double Has",
			path:    StartPath(qs).Has("status", "cool_person").Has("follows", "fred"),
			expect:  []string{"bob"},
		},
		{
			message: "use .Tag()-.Is()-.Back()",
			path:    StartPath(qs, "bob").In("follows").Tag("foo").Out("status").Is("cool_person").Back("foo"),
			expect:  []string{"dani"},
		},
		{
			message: "do multiple .Back()s",
			path:    StartPath(qs, "emily").Out("follows").Tag("f").Out("follows").Out("status").Is("cool_person").Back("f").In("follows").In("follows").Tag("acd").Out("status").Is("cool_person").Back("f"),
			tag:     "acd",
			expect:  []string{"dani"},
		},
		{
			message: "InPredicates()",
			path:    StartPath(qs, "bob").InPredicates(),
			expect:  []string{"follows"},
		},
		{
			message: "OutPredicates()",
			path:    StartPath(qs, "bob").OutPredicates(),
			expect:  []string{"follows", "status"},
		},
		// Morphism tests
		{
			message: "show simple morphism",
			path:    StartPath(qs, "charlie").Follow(grandfollows),
			expect:  []string{"greg", "fred", "bob"},
		},
		{
			message: "show reverse morphism",
			path:    StartPath(qs, "fred").FollowReverse(grandfollows),
			expect:  []string{"alice", "charlie", "dani"},
		},
	}
}

func TestMorphisms(t *testing.T) {
	qs := makeTestStore(t)
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
