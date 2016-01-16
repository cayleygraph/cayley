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

package pathtest

import (
	"io"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/quad"
	"github.com/google/cayley/quad/cquads"

	. "github.com/google/cayley/graph/path2"
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

func makeTestStore(t testing.TB, fnc func() graph.QuadStore) graph.QuadStore {
	simpleGraph := loadGraph("../../data/testdata.nq", t)
	qs := fnc()
	w, _ := graph.NewQuadWriter("single", qs, nil)
	for _, t := range simpleGraph {
		w.AddQuad(t)
	}
	return qs
}

func runTopLevel(qs graph.QuadStore, p PathObj) []string {
	var out []string
	if _, ok := p.(*Path); !ok {
		p = OptimizeOn(p, qs)
	}
	if p == nil {
		return nil
	}
	it := p.BuildIterator()
	it, _ = it.Optimize()
	for graph.Next(it) {
		v := qs.NameOf(it.Result())
		out = append(out, v)
	}
	return out
}

func runTag(qs graph.QuadStore, p PathObj, tag string) []string {
	var out []string
	if _, ok := p.(*Path); !ok {
		p = OptimizeOn(p, qs)
	}
	if p == nil {
		return nil
	}
	it := p.BuildIterator()
	it, _ = it.Optimize()
	for graph.Next(it) {
		tags := make(map[string]graph.Value)
		it.TagResults(tags)
		out = append(out, qs.NameOf(tags[tag]))
		for it.NextPath() {
			tags := make(map[string]graph.Value)
			it.TagResults(tags)
			out = append(out, qs.NameOf(tags[tag]))
		}
	}
	return out
}

type test struct {
	message string
	pathc   *Path
	path    Nodes
	expect  []string
	tag     string
}

// Define morphisms without a QuadStore

var (
	grandfollows     = StartMorphism().Out("follows").Out("follows")
	grandfollowsPath = Out{
		From: Out{
			From:   Start{},
			Via:    Fixed{"follows"},
			Labels: AllNodes{},
		},
		Via:    Fixed{"follows"},
		Labels: AllNodes{},
	}
)

func testSet(qs graph.QuadStore) []test {
	return []test{
		{
			message: "use out",
			path:    Out{From: Fixed{"alice"}, Via: Fixed{"follows"}, Labels: AllNodes{}},
			pathc:   StartPath(qs, "alice").Out("follows"),
			expect:  []string{"bob"},
		},
		{
			message: "use in",
			path:    Out{From: Fixed{"bob"}, Via: Fixed{"follows"}, Rev: true, Labels: AllNodes{}},
			pathc:   StartPath(qs, "bob").In("follows"),
			expect:  []string{"alice", "charlie", "dani"},
		},
		{
			message: "use path Out",
			path: Out{
				From: Fixed{"bob"},
				Via: Out{
					From:   Fixed{"predicates"},
					Via:    Fixed{"are"},
					Labels: AllNodes{},
				},
				Labels: AllNodes{},
			},
			pathc:  StartPath(qs, "bob").Out(StartPath(qs, "predicates").Out("are")),
			expect: []string{"fred", "cool_person"},
		},
		{
			message: "use And",
			path: IntersectNodes{
				Out{From: Fixed{"dani"}, Via: Fixed{"follows"}, Labels: AllNodes{}},
				Out{From: Fixed{"charlie"}, Via: Fixed{"follows"}, Labels: AllNodes{}},
			},
			pathc: StartPath(qs, "dani").Out("follows").And(
				StartPath(qs, "charlie").Out("follows")),
			expect: []string{"bob"},
		},
		{
			message: "use Or",
			path: UnionNodes{
				Out{From: Fixed{"fred"}, Via: Fixed{"follows"}, Labels: AllNodes{}},
				Out{From: Fixed{"alice"}, Via: Fixed{"follows"}, Labels: AllNodes{}},
			},
			pathc: StartPath(qs, "fred").Out("follows").Or(
				StartPath(qs, "alice").Out("follows")),
			expect: []string{"bob", "greg"},
		},
		{
			message: "implicit All",
			path:    AllNodes{},
			pathc:   StartPath(qs),
			expect:  []string{"alice", "bob", "charlie", "dani", "emily", "fred", "greg", "follows", "status", "cool_person", "predicates", "are", "smart_graph", "smart_person"},
		},
		{
			message: "follow",
			path: Follow(
				Fixed{"charlie"},
				Out{
					From: Out{
						From:   Start{},
						Via:    Fixed{"follows"},
						Labels: AllNodes{},
					},
					Via:    Fixed{"follows"},
					Labels: AllNodes{},
				},
			),
			pathc:  StartPath(qs, "charlie").Follow(StartMorphism().Out("follows").Out("follows")),
			expect: []string{"bob", "fred", "greg"},
		},
		{
			message: "followR",
			path: FollowReverse(
				Fixed{"fred"},
				Out{
					From: Out{
						From:   Start{},
						Via:    Fixed{"follows"},
						Labels: AllNodes{},
					},
					Via:    Fixed{"follows"},
					Labels: AllNodes{},
				},
			),
			pathc:  StartPath(qs, "fred").FollowReverse(StartMorphism().Out("follows").Out("follows")),
			expect: []string{"alice", "charlie", "dani"},
		},
		{
			message: "is, tag, instead of FollowR",
			path: IntersectNodes{
				Follow(
					Tag{
						Nodes: AllNodes{},
						Tags:  []string{"first"},
					},
					Out{
						From: Out{
							From:   Start{},
							Via:    Fixed{"follows"},
							Labels: AllNodes{},
						},
						Via:    Fixed{"follows"},
						Labels: AllNodes{},
					},
				),
				Fixed{"fred"},
			},
			pathc:  StartPath(qs).Tag("first").Follow(StartMorphism().Out("follows").Out("follows")).Is("fred"),
			expect: []string{"alice", "charlie", "dani"},
			tag:    "first",
		},
		{
			message: "use Except to filter out a single vertex",
			path: Except{
				From:  Fixed{"alice", "bob"},
				Nodes: Fixed{"alice"},
			},
			pathc:  StartPath(qs, "alice", "bob").Except(StartPath(qs, "alice")),
			expect: []string{"bob"},
		},
		{
			message: "use chained Except",
			path: Except{
				From: Except{
					From:  Fixed{"alice", "bob", "charlie"},
					Nodes: Fixed{"bob"},
				},
				Nodes: Fixed{"alice"},
			},
			pathc:  StartPath(qs, "alice", "bob", "charlie").Except(StartPath(qs, "bob")).Except(StartPath(qs, "alice")),
			expect: []string{"charlie"},
		},
		{
			message: "show a simple save",
			path: Save{
				From: AllNodes{},
				Via:  Fixed{"status"},
				Tags: []string{"somecool"},
			},
			pathc:  StartPath(qs).Save("status", "somecool"),
			tag:    "somecool",
			expect: []string{"cool_person", "cool_person", "cool_person", "smart_person", "smart_person"},
		},
		{
			message: "show a simple saveR",
			path: Save{
				From: Fixed{"cool_person"},
				Via:  Fixed{"status"},
				Tags: []string{"who"},
				Rev:  true,
			},
			pathc:  StartPath(qs, "cool_person").SaveReverse("status", "who"),
			tag:    "who",
			expect: []string{"greg", "dani", "bob"},
		},
		{
			message: "show a simple Has",
			path: Has{
				From:  AllNodes{},
				Via:   Fixed{"status"},
				Nodes: Fixed{"cool_person"},
			},
			pathc:  StartPath(qs).Has("status", "cool_person"),
			expect: []string{"greg", "dani", "bob"},
		},
		{
			message: "show a double Has",
			path: Has{
				From: Has{
					From:  AllNodes{},
					Via:   Fixed{"status"},
					Nodes: Fixed{"cool_person"},
				},
				Via:   Fixed{"follows"},
				Nodes: Fixed{"fred"},
			},
			pathc:  StartPath(qs).Has("status", "cool_person").Has("follows", "fred"),
			expect: []string{"bob"},
		},
		//		{
		//			message: "use .Tag()-.Is()-.Back()",
		//			path:    StartPath(qs, "bob").In("follows").Tag("foo").Out("status").Is("cool_person").Back("foo"),
		//			expect:  []string{"dani"},
		//		},
		//		{
		//			message: "do multiple .Back()s",
		//			path:    StartPath(qs, "emily").Out("follows").Tag("f").Out("follows").Out("status").Is("cool_person").Back("f").In("follows").In("follows").Tag("acd").Out("status").Is("cool_person").Back("f"),
		//			tag:     "acd",
		//			expect:  []string{"dani"},
		//		},
		{
			message: "InPredicates()",
			path: Predicates{
				From: Fixed{"bob"},
				Rev:  true,
			},
			pathc:  StartPath(qs, "bob").InPredicates(),
			expect: []string{"follows"},
		},
		{
			message: "OutPredicates()",
			path: Predicates{
				From: Fixed{"bob"},
			},
			pathc:  StartPath(qs, "bob").OutPredicates(),
			expect: []string{"follows", "status"},
		},
		// Morphism tests
		{
			message: "show simple morphism",
			path:    Follow(Fixed{"charlie"}, grandfollowsPath),
			pathc:   StartPath(qs, "charlie").Follow(grandfollows),
			expect:  []string{"greg", "fred", "bob"},
		},
		{
			message: "show reverse morphism",
			path:    FollowReverse(Fixed{"fred"}, grandfollowsPath),
			pathc:   StartPath(qs, "fred").FollowReverse(grandfollows),
			expect:  []string{"alice", "charlie", "dani"},
		},
		// Context tests
		{
			message: "query without label limitation",
			path: Out{
				From:   Fixed{"greg"},
				Via:    Fixed{"status"},
				Labels: AllNodes{},
			},
			pathc:  StartPath(qs, "greg").Out("status"),
			expect: []string{"smart_person", "cool_person"},
		},
		{
			message: "query with label limitation",
			path: Out{
				From:   Fixed{"greg"},
				Via:    Fixed{"status"},
				Labels: Fixed{"smart_graph"},
			},
			pathc:  StartPath(qs, "greg").LabelContext("smart_graph").Out("status"),
			expect: []string{"smart_person"},
		},
		//		{
		//			message: "reverse context",
		//			path:    StartPath(qs, "greg").Tag("base").LabelContext("smart_graph").Out("status").Tag("status").Back("base"),
		//			expect:  []string{"greg"},
		//		},
	}
}

func TestMorphisms(t testing.TB, fnc func() graph.QuadStore) {
	qs := makeTestStore(t, fnc)
	for _, test := range testSet(qs) {
		t.Log(test.message)
		var got, gotc []string
		if test.tag == "" {
			got = runTopLevel(qs, test.path)
			if test.pathc != nil {
				gotc = runTopLevel(qs, test.pathc)
			}
		} else {
			got = runTag(qs, test.path, test.tag)
			if test.pathc != nil {
				gotc = runTag(qs, test.pathc, test.tag)
			}
		}
		sort.Strings(got)
		sort.Strings(gotc)
		sort.Strings(test.expect)
		if !reflect.DeepEqual(got, test.expect) {
			t.Errorf("Failed to %s, got: %v expected: %v", test.message, got, test.expect)
		}
		if test.pathc != nil && !reflect.DeepEqual(gotc, test.expect) {
			t.Errorf("Failed to %s (compat), got: %v expected: %v", test.message, gotc, test.expect)
		}
	}
}
