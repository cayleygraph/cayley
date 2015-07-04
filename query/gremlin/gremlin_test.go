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

package gremlin

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

// This is a simple test graph used for testing the gremlin queries
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
//

func makeTestSession(data []quad.Quad) *Session {
	qs, _ := graph.NewQuadStore("memstore", "", nil)
	w, _ := graph.NewQuadWriter("single", qs, nil)
	for _, t := range data {
		w.AddQuad(t)
	}
	return NewSession(qs, -1, false)
}

var testQueries = []struct {
	message string
	query   string
	tag     string
	expect  []string
}{
	// Simple query tests.
	{
		message: "get a single vertex",
		query: `
			g.V("alice").All()
		`,
		expect: []string{"alice"},
	},
	{
		message: "use .Out()",
		query: `
			g.V("alice").Out("follows").All()
		`,
		expect: []string{"bob"},
	},
	{
		message: "use .In()",
		query: `
			g.V("bob").In("follows").All()
		`,
		expect: []string{"alice", "charlie", "dani"},
	},
	{
		message: "use .Both()",
		query: `
			g.V("fred").Both("follows").All()
		`,
		expect: []string{"bob", "greg", "emily"},
	},
	{
		message: "use .Tag()-.Is()-.Back()",
		query: `
			g.V("bob").In("follows").Tag("foo").Out("status").Is("cool_person").Back("foo").All()
		`,
		expect: []string{"dani"},
	},
	{
		message: "separate .Tag()-.Is()-.Back()",
		query: `
			x = g.V("charlie").Out("follows").Tag("foo").Out("status").Is("cool_person").Back("foo")
			x.In("follows").Is("dani").Back("foo").All()
		`,
		expect: []string{"bob"},
	},
	{
		message: "do multiple .Back()s",
		query: `
			g.V("emily").Out("follows").As("f").Out("follows").Out("status").Is("cool_person").Back("f").In("follows").In("follows").As("acd").Out("status").Is("cool_person").Back("f").All()
		`,
		tag:    "acd",
		expect: []string{"dani"},
	},
	{
		message: "use Except to filter out a single vertex",
		query: `
			g.V("alice", "bob").Except(g.V("alice")).All()
		`,
		expect: []string{"bob"},
	},
	{
		message: "use chained Except",
		query: `
			g.V("alice", "bob", "charlie").Except(g.V("bob")).Except(g.V("charlie")).All()
		`,
		expect: []string{"alice"},
	},

	// Morphism tests.
	{
		message: "show simple morphism",
		query: `
			grandfollows = g.M().Out("follows").Out("follows")
			g.V("charlie").Follow(grandfollows).All()
		`,
		expect: []string{"greg", "fred", "bob"},
	},
	{
		message: "show reverse morphism",
		query: `
			grandfollows = g.M().Out("follows").Out("follows")
			g.V("fred").FollowR(grandfollows).All()
		`,
		expect: []string{"alice", "charlie", "dani"},
	},

	// Intersection tests.
	{
		message: "show simple intersection",
		query: `
			function follows(x) { return g.V(x).Out("follows") }
			follows("dani").And(follows("charlie")).All()
		`,
		expect: []string{"bob"},
	},
	{
		message: "show simple morphism intersection",
		query: `
			grandfollows = g.M().Out("follows").Out("follows")
			function gfollows(x) { return g.V(x).Follow(grandfollows) }
			gfollows("alice").And(gfollows("charlie")).All()
		`,
		expect: []string{"fred"},
	},
	{
		message: "show double morphism intersection",
		query: `
			grandfollows = g.M().Out("follows").Out("follows")
			function gfollows(x) { return g.V(x).Follow(grandfollows) }
			gfollows("emily").And(gfollows("charlie")).And(gfollows("bob")).All()
		`,
		expect: []string{"greg"},
	},
	{
		message: "show reverse intersection",
		query: `
			grandfollows = g.M().Out("follows").Out("follows")
			g.V("greg").FollowR(grandfollows).Intersect(g.V("fred").FollowR(grandfollows)).All()
		`,
		expect: []string{"charlie"},
	},
	{
		message: "show standard sort of morphism intersection, continue follow",
		query: `gfollowers = g.M().In("follows").In("follows")
			function cool(x) { return g.V(x).As("a").Out("status").Is("cool_person").Back("a") }
			cool("greg").Follow(gfollowers).Intersect(cool("bob").Follow(gfollowers)).All()
		`,
		expect: []string{"charlie"},
	},

	// Gremlin Has tests.
	{
		message: "show a simple Has",
		query: `
				g.V().Has("status", "cool_person").All()
		`,
		expect: []string{"greg", "dani", "bob"},
	},
	{
		message: "show a double Has",
		query: `
				g.V().Has("status", "cool_person").Has("follows", "fred").All()
		`,
		expect: []string{"bob"},
	},

	// Tag tests.
	{
		message: "show a simple save",
		query: `
			g.V().Save("status", "somecool").All()
		`,
		tag:    "somecool",
		expect: []string{"cool_person", "cool_person", "cool_person"},
	},
	{
		message: "show a simple saveR",
		query: `
			g.V("cool_person").SaveR("status", "who").All()
		`,
		tag:    "who",
		expect: []string{"greg", "dani", "bob"},
	},
	{
		message: "show an out save",
		query: `
			g.V("dani").Out(null, "pred").All()
		`,
		tag:    "pred",
		expect: []string{"follows", "follows", "status"},
	},
	{
		message: "show a tag list",
		query: `
			g.V("dani").Out(null, ["pred", "foo", "bar"]).All()
		`,
		tag:    "foo",
		expect: []string{"follows", "follows", "status"},
	},
	{
		message: "show a pred list",
		query: `
			g.V("dani").Out(["follows", "status"]).All()
		`,
		expect: []string{"bob", "greg", "cool_person"},
	},
	{
		message: "show a predicate path",
		query: `
			g.V("dani").Out(g.V("follows"), "pred").All()
		`,
		expect: []string{"bob", "greg"},
	},
	{
		message: "list all bob's incoming predicates",
		query: `
		  g.V("bob").InPredicates().All()
		`,
		expect: []string{"follows"},
	},
	{
		message: "list all in predicates",
		query: `
		  g.V().InPredicates().All()
		`,
		expect: []string{"follows", "status"},
	},
	{
		message: "list all out predicates",
		query: `
		  g.V().OutPredicates().All()
		`,
		expect: []string{"follows", "status"},
	},
}

func runQueryGetTag(g []quad.Quad, query string, tag string) []string {
	js := makeTestSession(g)
	c := make(chan interface{}, 5)
	js.Execute(query, c, -1)

	var results []string
	for res := range c {
		data := res.(*Result)
		if data.val == nil {
			val := data.actualResults[tag]
			if val != nil {
				results = append(results, js.qs.NameOf(val))
			}
		}
	}

	return results
}

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

func TestGremlin(t *testing.T) {
	simpleGraph := loadGraph("../../data/testdata.nq", t)
	for _, test := range testQueries {
		if test.tag == "" {
			test.tag = TopResultTag
		}
		got := runQueryGetTag(simpleGraph, test.query, test.tag)
		sort.Strings(got)
		sort.Strings(test.expect)
		if !reflect.DeepEqual(got, test.expect) {
			t.Errorf("Failed to %s, got: %v expected: %v", test.message, got, test.expect)
		}
	}
}

var issue160TestGraph = []quad.Quad{
	{"alice", "follows", "bob", ""},
	{"bob", "follows", "alice", ""},
	{"charlie", "follows", "bob", ""},
	{"dani", "follows", "charlie", ""},
	{"dani", "follows", "alice", ""},
	{"alice", "is", "cool", ""},
	{"bob", "is", "not cool", ""},
	{"charlie", "is", "cool", ""},
	{"danie", "is", "not cool", ""},
}

func TestIssue160(t *testing.T) {
	query := `g.V().Tag('query').Out('follows').Out('follows').ForEach(function (item) { if (item.id !== item.query) g.Emit({ id: item.id }); })`
	expect := []string{
		"****\nid : alice\n",
		"****\nid : bob\n",
		"****\nid : bob\n",
		"=> <nil>\n",
	}

	ses := makeTestSession(issue160TestGraph)
	c := make(chan interface{}, 5)
	go ses.Execute(query, c, 100)
	var got []string
	for res := range c {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Unexpected panic: %v", r)
				}
			}()
			got = append(got, ses.Format(res))
		}()
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, expect) {
		t.Errorf("Unexpected result, got: %q expected: %q", got, expect)
	}
}
