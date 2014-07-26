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
	"reflect"
	"sort"
	"testing"

	"github.com/google/cayley/graph"
	_ "github.com/google/cayley/graph/memstore"
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
var simpleGraph = []*graph.Triple{
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
}

func makeTestSession(data []*graph.Triple) *Session {
	ts, _ := graph.NewTripleStore("memstore", "", nil)
	for _, t := range data {
		ts.AddTriple(t)
	}
	return NewSession(ts, -1, false)
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
			g.V("A").All()
		`,
		expect: []string{"A"},
	},
	{
		message: "use .Out()",
		query: `
			g.V("A").Out("follows").All()
		`,
		expect: []string{"B"},
	},
	{
		message: "use .In()",
		query: `
			g.V("B").In("follows").All()
		`,
		expect: []string{"A", "C", "D"},
	},
	{
		message: "use .Both()",
		query: `
			g.V("F").Both("follows").All()
		`,
		expect: []string{"B", "G", "E"},
	},
	{
		message: "use .Tag()-.Is()-.Back()",
		query: `
			g.V("B").In("follows").Tag("foo").Out("status").Is("cool").Back("foo").All()
		`,
		expect: []string{"D"},
	},
	{
		message: "separate .Tag()-.Is()-.Back()",
		query: `
			x = g.V("C").Out("follows").Tag("foo").Out("status").Is("cool").Back("foo")
			x.In("follows").Is("D").Back("foo").All()
		`,
		expect: []string{"B"},
	},
	{
		message: "do multiple .Back()s",
		query: `
			g.V("E").Out("follows").As("f").Out("follows").Out("status").Is("cool").Back("f").In("follows").In("follows").As("acd").Out("status").Is("cool").Back("f").All()
		`,
		tag:    "acd",
		expect: []string{"D"},
	},

	// Morphism tests.
	{
		message: "show simple morphism",
		query: `
			grandfollows = g.M().Out("follows").Out("follows")
			g.V("C").Follow(grandfollows).All()
		`,
		expect: []string{"G", "F", "B"},
	},
	{
		message: "show reverse morphism",
		query: `
			grandfollows = g.M().Out("follows").Out("follows")
			g.V("F").FollowR(grandfollows).All()
		`,
		expect: []string{"A", "C", "D"},
	},

	// Intersection tests.
	{
		message: "show simple intersection",
		query: `
			function follows(x) { return g.V(x).Out("follows") }
			follows("D").And(follows("C")).All()
		`,
		expect: []string{"B"},
	},
	{
		message: "show simple morphism intersection",
		query: `
			grandfollows = g.M().Out("follows").Out("follows")
			function gfollows(x) { return g.V(x).Follow(grandfollows) }
			gfollows("A").And(gfollows("C")).All()
		`,
		expect: []string{"F"},
	},
	{
		message: "show double morphism intersection",
		query: `
			grandfollows = g.M().Out("follows").Out("follows")
			function gfollows(x) { return g.V(x).Follow(grandfollows) }
			gfollows("E").And(gfollows("C")).And(gfollows("B")).All()
		`,
		expect: []string{"G"},
	},
	{
		message: "show reverse intersection",
		query: `
			grandfollows = g.M().Out("follows").Out("follows")
			g.V("G").FollowR(grandfollows).Intersect(g.V("F").FollowR(grandfollows)).All()
		`,
		expect: []string{"C"},
	},
	{
		message: "show standard sort of morphism intersection, continue follow",
		query: `gfollowers = g.M().In("follows").In("follows")
			function cool(x) { return g.V(x).As("a").Out("status").Is("cool").Back("a") }
			cool("G").Follow(gfollowers).Intersect(cool("B").Follow(gfollowers)).All()
		`,
		expect: []string{"C"},
	},

	// Gremlin Has tests.
	{
		message: "show a simple Has",
		query: `
				g.V().Has("status", "cool").All()
		`,
		expect: []string{"G", "D", "B"},
	},
	{
		message: "show a double Has",
		query: `
				g.V().Has("status", "cool").Has("follows", "F").All()
		`,
		expect: []string{"B"},
	},

	// Tag tests.
	{
		message: "show a simple save",
		query: `
			g.V().Save("status", "somecool").All()
		`,
		tag:    "somecool",
		expect: []string{"cool", "cool", "cool"},
	},
	{
		message: "show a simple saveR",
		query: `
			g.V("cool").SaveR("status", "who").All()
		`,
		tag:    "who",
		expect: []string{"G", "D", "B"},
	},
	{
		message: "show an out save",
		query: `
			g.V("D").Out(null, "pred").All()
		`,
		tag:    "pred",
		expect: []string{"follows", "follows", "status"},
	},
	{
		message: "show a tag list",
		query: `
			g.V("D").Out(null, ["pred", "foo", "bar"]).All()
		`,
		tag:    "foo",
		expect: []string{"follows", "follows", "status"},
	},
	{
		message: "show a pred list",
		query: `
			g.V("D").Out(["follows", "status"]).All()
		`,
		expect: []string{"B", "G", "cool"},
	},
	{
		message: "show a predicate path",
		query: `
			g.V("D").Out(g.V("follows"), "pred").All()
		`,
		expect: []string{"B", "G"},
	},
}

func runQueryGetTag(g []*graph.Triple, query string, tag string) []string {
	js := makeTestSession(g)
	c := make(chan interface{}, 5)
	js.ExecInput(query, c, -1)

	var results []string
	for res := range c {
		data := res.(*GremlinResult)
		if data.val == nil {
			val := (*data.actualResults)[tag]
			if val != nil {
				results = append(results, js.ts.NameOf(val))
			}
		}
	}

	return results
}

func TestGremlin(t *testing.T) {
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
