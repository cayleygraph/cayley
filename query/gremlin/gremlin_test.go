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
	"fmt"
	"reflect"
	"sort"
	"testing"

	"golang.org/x/net/context"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest"
	_ "github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/query"
	_ "github.com/cayleygraph/cayley/writer"
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
	return NewSession(qs, false)
}

var testQueries = []struct {
	message string
	query   string
	tag     string
	expect  []string
	err     bool // TODO(dennwc): define error types for Gremlin and handle them
}{
	// Simple query tests.
	{
		message: "get a single vertex",
		query: `
			g.V("<alice>").All()
		`,
		expect: []string{"<alice>"},
	},
	{
		message: "use .GetLimit",
		query: `
			g.V().GetLimit(5)
		`,
		expect: []string{"<alice>", "<bob>", "<follows>", "<fred>", "<status>"},
	},
	{
		message: "get a single vertex (IRI)",
		query: `
			g.V(iri("alice")).All()
		`,
		expect: []string{"<alice>"},
	},
	{
		message: "use .Out()",
		query: `
			g.V("<alice>").Out("<follows>").All()
		`,
		expect: []string{"<bob>"},
	},
	{
		message: "use .Out() (IRI)",
		query: `
			g.V(iri("alice")).Out(iri("follows")).All()
		`,
		expect: []string{"<bob>"},
	},
	{
		message: "use .Out() (any)",
		query: `
			g.V("<bob>").Out().All()
		`,
		expect: []string{"<fred>", "cool_person"},
	},
	{
		message: "use .In()",
		query: `
			g.V("<bob>").In("<follows>").All()
		`,
		expect: []string{"<alice>", "<charlie>", "<dani>"},
	},
	{
		message: "use .In() (any)",
		query: `
			g.V("<bob>").In().All()
		`,
		expect: []string{"<alice>", "<charlie>", "<dani>"},
	},
	{
		message: "use .In() with .Filter()",
		query: `
			g.V("<bob>").In("<follows>").Filter(gt(iri("c")),lt(iri("d"))).All()
		`,
		expect: []string{"<charlie>"},
	},
	{
		message: "use .In() with .Filter(regex)",
		query: `
			g.V("<bob>").In("<follows>").Filter(regex("ar?li.*e")).All()
		`,
		expect: nil,
	},
	{
		message: "use .In() with .Filter(regex with IRIs)",
		query: `
			g.V("<bob>").In("<follows>").Filter(regex("ar?li.*e", true)).All()
		`,
		expect: []string{"<alice>", "<charlie>"},
	},
	{
		message: "use .In() with .Filter(regex with IRIs)",
		query: `
			g.V("<bob>").In("<follows>").Filter(regex(iri("ar?li.*e"))).All()
		`,
		err: true,
	},
	{
		message: "use .In() with .Filter(regex,gt)",
		query: `
			g.V("<bob>").In("<follows>").Filter(regex("ar?li.*e", true),gt(iri("c"))).All()
		`,
		expect: []string{"<charlie>"},
	},
	{
		message: "use .Both()",
		query: `
			g.V("<fred>").Both("<follows>").All()
		`,
		expect: []string{"<bob>", "<greg>", "<emily>"},
	},
	{
		message: "use .Tag()-.Is()-.Back()",
		query: `
			g.V("<bob>").In("<follows>").Tag("foo").Out("<status>").Is("cool_person").Back("foo").All()
		`,
		expect: []string{"<dani>"},
	},
	{
		message: "separate .Tag()-.Is()-.Back()",
		query: `
			x = g.V("<charlie>").Out("<follows>").Tag("foo").Out("<status>").Is("cool_person").Back("foo")
			x.In("<follows>").Is("<dani>").Back("foo").All()
		`,
		expect: []string{"<bob>"},
	},
	{
		message: "do multiple .Back()s",
		query: `
			g.V("<emily>").Out("<follows>").As("f").Out("<follows>").Out("<status>").Is("cool_person").Back("f").In("<follows>").In("<follows>").As("acd").Out("<status>").Is("cool_person").Back("f").All()
		`,
		tag:    "acd",
		expect: []string{"<dani>"},
	},
	{
		message: "use Except to filter out a single vertex",
		query: `
			g.V("<alice>", "<bob>").Except(g.V("<alice>")).All()
		`,
		expect: []string{"<bob>"},
	},
	{
		message: "use chained Except",
		query: `
			g.V("<alice>", "<bob>", "<charlie>").Except(g.V("<bob>")).Except(g.V("<charlie>")).All()
		`,
		expect: []string{"<alice>"},
	},

	{
		message: "use Unique",
		query: `
			g.V("<alice>", "<bob>", "<charlie>").Out("<follows>").Unique().All()
		`,
		expect: []string{"<bob>", "<dani>", "<fred>"},
	},

	// Morphism tests.
	{
		message: "show simple morphism",
		query: `
			grandfollows = g.M().Out("<follows>").Out("<follows>")
			g.V("<charlie>").Follow(grandfollows).All()
		`,
		expect: []string{"<greg>", "<fred>", "<bob>"},
	},
	{
		message: "show reverse morphism",
		query: `
			grandfollows = g.M().Out("<follows>").Out("<follows>")
			g.V("<fred>").FollowR(grandfollows).All()
		`,
		expect: []string{"<alice>", "<charlie>", "<dani>"},
	},

	// Intersection tests.
	{
		message: "show simple intersection",
		query: `
			function follows(x) { return g.V(x).Out("<follows>") }
			follows("<dani>").And(follows("<charlie>")).All()
		`,
		expect: []string{"<bob>"},
	},
	{
		message: "show simple morphism intersection",
		query: `
			grandfollows = g.M().Out("<follows>").Out("<follows>")
			function gfollows(x) { return g.V(x).Follow(grandfollows) }
			gfollows("<alice>").And(gfollows("<charlie>")).All()
		`,
		expect: []string{"<fred>"},
	},
	{
		message: "show double morphism intersection",
		query: `
			grandfollows = g.M().Out("<follows>").Out("<follows>")
			function gfollows(x) { return g.V(x).Follow(grandfollows) }
			gfollows("<emily>").And(gfollows("<charlie>")).And(gfollows("<bob>")).All()
		`,
		expect: []string{"<greg>"},
	},
	{
		message: "show reverse intersection",
		query: `
			grandfollows = g.M().Out("<follows>").Out("<follows>")
			g.V("<greg>").FollowR(grandfollows).Intersect(g.V("<fred>").FollowR(grandfollows)).All()
		`,
		expect: []string{"<charlie>"},
	},
	{
		message: "show standard sort of morphism intersection, continue follow",
		query: `gfollowers = g.M().In("<follows>").In("<follows>")
			function cool(x) { return g.V(x).As("a").Out("<status>").Is("cool_person").Back("a") }
			cool("<greg>").Follow(gfollowers).Intersect(cool("<bob>").Follow(gfollowers)).All()
		`,
		expect: []string{"<charlie>"},
	},
	{
		message: "test Or()",
		query: `
			g.V("<bob>").Out("<follows>").Or(g.V().Has("<status>", "cool_person")).All()
		`,
		expect: []string{"<fred>", "<bob>", "<greg>", "<dani>"},
	},

	// Gremlin Has tests.
	{
		message: "show a simple Has",
		query: `
				g.V().Has("<status>", "cool_person").All()
		`,
		expect: []string{"<greg>", "<dani>", "<bob>"},
	},
	{
		message: "show a simple HasR",
		query: `
				g.V().HasR("<status>", "<bob>").All()
		`,
		expect: []string{"cool_person"},
	},
	{
		message: "show a double Has",
		query: `
				g.V().Has("<status>", "cool_person").Has("<follows>", "<fred>").All()
		`,
		expect: []string{"<bob>"},
	},

	// Gremlin Skip/Limit tests.
	{
		message: "use Limit",
		query: `
				g.V().Has("<status>", "cool_person").Limit(2).All()
		`,
		expect: []string{"<bob>", "<dani>"},
	},
	{
		message: "use Skip",
		query: `
				g.V().Has("<status>", "cool_person").Skip(2).All()
		`,
		expect: []string{"<greg>"},
	},
	{
		message: "use Skip and Limit",
		query: `
				g.V().Has("<status>", "cool_person").Skip(1).Limit(1).All()
		`,
		expect: []string{"<dani>"},
	},

	{
		message: "show Count",
		query: `
				g.V().Has("<status>").Count().All()
		`,
		expect: []string{`"5"^^<http://schema.org/Integer>`},
	},
	{
		message: "use Count value",
		query: `
				g.Emit(g.V().Has("<status>").Count().ToValue()+1)
		`,
		expect: []string{"6"},
	},

	// Tag tests.
	{
		message: "show a simple save",
		query: `
			g.V().Save("<status>", "somecool").All()
		`,
		tag:    "somecool",
		expect: []string{"cool_person", "cool_person", "cool_person", "smart_person", "smart_person"},
	},
	{
		message: "show a simple saveR",
		query: `
			g.V("cool_person").SaveR("<status>", "who").All()
		`,
		tag:    "who",
		expect: []string{"<greg>", "<dani>", "<bob>"},
	},
	{
		message: "show an out save",
		query: `
			g.V("<dani>").Out(null, "pred").All()
		`,
		tag:    "pred",
		expect: []string{"<follows>", "<follows>", "<status>"},
	},
	{
		message: "show a tag list",
		query: `
			g.V("<dani>").Out(null, ["pred", "foo", "bar"]).All()
		`,
		tag:    "foo",
		expect: []string{"<follows>", "<follows>", "<status>"},
	},
	{
		message: "show a pred list",
		query: `
			g.V("<dani>").Out(["<follows>", "<status>"]).All()
		`,
		expect: []string{"<bob>", "<greg>", "cool_person"},
	},
	{
		message: "show a predicate path",
		query: `
			g.V("<dani>").Out(g.V("<follows>"), "pred").All()
		`,
		expect: []string{"<bob>", "<greg>"},
	},
	{
		message: "list all bob's incoming predicates",
		query: `
		  g.V("<bob>").InPredicates().All()
		`,
		expect: []string{"<follows>"},
	},
	{
		message: "list all in predicates",
		query: `
		  g.V().InPredicates().All()
		`,
		expect: []string{"<are>", "<follows>", "<status>"},
	},
	{
		message: "list all out predicates",
		query: `
		  g.V().OutPredicates().All()
		`,
		expect: []string{"<are>", "<follows>", "<status>"},
	},
	{
		message: "traverse using LabelContext",
		query: `
			g.V("<greg>").LabelContext("<smart_graph>").Out("<status>").All()
		`,
		expect: []string{"smart_person"},
	},
	{
		message: "open and close a LabelContext",
		query: `
			g.V().LabelContext("<smart_graph>").In("<status>").LabelContext(null).In("<follows>").All()
		`,
		expect: []string{"<dani>", "<fred>"},
	},
	{
		message: "issue #254",
		query:   `g.V({"id":"<alice>"}).All()`,
		expect:  nil, err: true,
	},
	{
		message: "roundtrip values",
		query: `
		v = g.V("<bob>").ToValue()
		s = g.V(v).Out("<status>").ToValue()
		g.V(s).All()
		`,
		expect: []string{"cool_person"},
	},
	{
		message: "roundtrip values (tag map)",
		query: `
		v = g.V("<bob>").TagValue()
		s = g.V(v.id).Out("<status>").TagValue()
		g.V(s.id).All()
		`,
		expect: []string{"cool_person"},
	},
	{
		message: "show ToArray",
		query: `
			arr = g.V("<bob>").In("<follows>").ToArray()
			for (i in arr) g.Emit(arr[i]);
		`,
		expect: []string{"<alice>", "<charlie>", "<dani>"},
	},
	{
		message: "show ToArray with limit",
		query: `
			arr = g.V("<bob>").In("<follows>").ToArray(2)
			for (i in arr) g.Emit(arr[i]);
		`,
		expect: []string{"<alice>", "<charlie>"},
	},
	{
		message: "clone paths",
		query: `
			var alice = g.V('<alice>')
			g.Emit(alice.ToValue())
			var out = alice.Out('<follows>')
			g.Emit(out.ToValue())
			g.Emit(alice.ToValue())
		`,
		expect: []string{"<alice>", "<bob>", "<alice>"},
	},
}

func runQueryGetTag(rec func(), g []quad.Quad, qu string, tag string) ([]string, error) {
	js := makeTestSession(g)
	c := make(chan query.Result, 1)
	go func() {
		defer rec()
		js.Execute(context.TODO(), qu, c, -1)
	}()

	var results []string
	for res := range c {
		data := res.(*Result)
		if data.err != nil {
			return results, data.err
		} else if data.val == nil {
			if val := data.actualResults[tag]; val != nil {
				results = append(results, quadValueToString(js.qs.NameOf(val)))
			}
		} else {
			switch v := data.val.(type) {
			case string:
				results = append(results, v)
			default:
				results = append(results, fmt.Sprint(v))
			}
		}
	}
	return results, nil
}

func TestGremlin(t *testing.T) {
	simpleGraph := graphtest.LoadGraph(t, "../../data/testdata.nq")
	for _, test := range testQueries {
		func() {
			rec := func() {
				if r := recover(); r != nil {
					t.Errorf("Unexpected panic on %s: %v", test.message, r)
				}
			}
			defer rec()
			if test.tag == "" {
				test.tag = TopResultTag
			}
			got, err := runQueryGetTag(rec, simpleGraph, test.query, test.tag)
			if err != nil {
				if test.err {
					return //expected
				}
				t.Errorf("unexpected error on %s: %v", test.message, err)
			}
			sort.Strings(got)
			sort.Strings(test.expect)
			t.Log("testing", test.message)
			if !reflect.DeepEqual(got, test.expect) {
				t.Errorf("Failed to %s, got: %v expected: %v", test.message, got, test.expect)
			}
		}()
	}
}

var issue160TestGraph = []quad.Quad{
	quad.MakeRaw("alice", "follows", "bob", ""),
	quad.MakeRaw("bob", "follows", "alice", ""),
	quad.MakeRaw("charlie", "follows", "bob", ""),
	quad.MakeRaw("dani", "follows", "charlie", ""),
	quad.MakeRaw("dani", "follows", "alice", ""),
	quad.MakeRaw("alice", "is", "cool", ""),
	quad.MakeRaw("bob", "is", "not cool", ""),
	quad.MakeRaw("charlie", "is", "cool", ""),
	quad.MakeRaw("danie", "is", "not cool", ""),
}

func TestIssue160(t *testing.T) {
	qu := `g.V().Tag('query').Out(raw('follows')).Out(raw('follows')).ForEach(function (item) { if (item.id !== item.query) g.Emit({ id: item.id }); })`
	expect := []string{
		"****\nid : alice\n",
		"****\nid : bob\n",
		"****\nid : bob\n",
		"=> <nil>\n",
	}

	ses := makeTestSession(issue160TestGraph)
	c := make(chan query.Result, 5)
	go ses.Execute(context.TODO(), qu, c, 100)
	var got []string
	for res := range c {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Unexpected panic: %v", r)
				}
			}()
			got = append(got, ses.FormatREPL(res))
		}()
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, expect) {
		t.Errorf("Unexpected result, got: %q expected: %q", got, expect)
	}
}
