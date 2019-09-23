// Copyright 2017 The Cayley Authors. All rights reserved.
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

package gizmo

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest/testutil"
	_ "github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/cayley/query"
	_ "github.com/cayleygraph/cayley/writer"
	"github.com/cayleygraph/quad"

	// register global namespace for tests
	_ "github.com/cayleygraph/quad/voc/rdf"
)

// This is a simple test graph used for testing
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
	return NewSession(qs)
}

func intVal(v int) string {
	return quad.Int(v).String()
}

const multiGraphTestFile = "../../data/testdata_multigraph.nq"

var testQueries = []struct {
	message string
	data    []quad.Quad
	query   string
	limit   int
	tag     string
	file    string
	expect  []string
	err     bool // TODO(dennwc): define error types for Gizmo and handle them
}{
	// Simple query tests.
	{
		message: "get a single vertex",
		query: `
			g.V("<alice>").all()
		`,
		expect: []string{"<alice>"},
	},
	{
		message: "get a single vertex (legacy)",
		query: `
			g.V("<alice>").All()
		`,
		expect: []string{"<alice>"},
	},
	{
		message: "use .getLimit",
		query: `
			g.V().getLimit(5)
		`,
		expect: []string{"<alice>", "<bob>", "<follows>", "<fred>", "<status>"},
	},
	{
		message: "get a single vertex (IRI)",
		query: `
			g.V(iri("alice")).all()
		`,
		expect: []string{"<alice>"},
	},
	{
		message: "use .out()",
		query: `
			g.V("<alice>").out("<follows>").all()
		`,
		expect: []string{"<bob>"},
	},
	{
		message: "use .out() (IRI)",
		query: `
			g.V(iri("alice")).out(iri("follows")).all()
		`,
		expect: []string{"<bob>"},
	},
	{
		message: "use .out() (any)",
		query: `
			g.V("<bob>").out().all()
		`,
		expect: []string{"<fred>", "cool_person"},
	},
	{
		message: "use .in()",
		query: `
			g.V("<bob>").in("<follows>").all()
		`,
		expect: []string{"<alice>", "<charlie>", "<dani>"},
	},
	{
		message: "use .in() (any)",
		query: `
			g.V("<bob>").in().all()
		`,
		expect: []string{"<alice>", "<charlie>", "<dani>"},
	},
	{
		message: "use .in() with .filter()",
		query: `
			g.V("<bob>").in("<follows>").filter(gt(iri("c")),lt(iri("d"))).all()
		`,
		expect: []string{"<charlie>"},
	},
	{
		message: "use .in() with .filter(regex)",
		query: `
			g.V("<bob>").in("<follows>").filter(regex("ar?li.*e")).all()
		`,
		expect: nil,
	},
	{
		message: "use .in() with .filter(prefix)",
		query: `
			g.V("<bob>").in("<follows>").filter(like("al%")).all()
		`,
		expect: []string{"<alice>"},
	},
	{
		message: "use .in() with .filter(wildcard)",
		query: `
			g.V("<bob>").in("<follows>").filter(like("a?i%e")).all()
		`,
		expect: []string{"<alice>"},
	},
	{
		message: "use .in() with .filter(regex with IRIs)",
		query: `
			g.V("<bob>").in("<follows>").filter(regex("ar?li.*e", true)).all()
		`,
		expect: []string{"<alice>", "<charlie>"},
	},
	{
		message: "use .in() with .filter(regex with IRIs)",
		query: `
			g.V("<bob>").in("<follows>").filter(regex(iri("ar?li.*e"))).all()
		`,
		err: true,
	},
	{
		message: "use .in() with .filter(regex,gt)",
		query: `
			g.V("<bob>").in("<follows>").filter(regex("ar?li.*e", true),gt(iri("c"))).all()
		`,
		expect: []string{"<charlie>"},
	},
	{
		message: "use .both()",
		query: `
			g.V("<fred>").both("<follows>").all()
		`,
		expect: []string{"<bob>", "<greg>", "<emily>"},
	},
	{
		message: "use .both() with tag",
		query: `
			g.V("<fred>").both(null, "pred").all()
		`,
		tag:    "pred",
		expect: []string{"<follows>", "<follows>", "<follows>"},
	},
	{
		message: "use .tag()-.is()-.back()",
		query: `
			g.V("<bob>").in("<follows>").tag("foo").out("<status>").is("cool_person").back("foo").all()
		`,
		expect: []string{"<dani>"},
	},
	{
		message: "separate .tag()-.is()-.back()",
		query: `
			x = g.V("<charlie>").out("<follows>").tag("foo").out("<status>").is("cool_person").back("foo")
			x.in("<follows>").is("<dani>").back("foo").all()
		`,
		expect: []string{"<bob>"},
	},
	{
		message: "do multiple .back()",
		query: `
			g.V("<emily>").out("<follows>").as("f").out("<follows>").out("<status>").is("cool_person").back("f").in("<follows>").in("<follows>").as("acd").out("<status>").is("cool_person").back("f").all()
		`,
		tag:    "acd",
		expect: []string{"<dani>"},
	},
	{
		message: "use Except to filter out a single vertex",
		query: `
			g.V("<alice>", "<bob>").except(g.V("<alice>")).all()
		`,
		expect: []string{"<bob>"},
	},
	{
		message: "use chained Except",
		query: `
			g.V("<alice>", "<bob>", "<charlie>").except(g.V("<bob>")).except(g.V("<charlie>")).all()
		`,
		expect: []string{"<alice>"},
	},

	{
		message: "use Unique",
		query: `
			g.V("<alice>", "<bob>", "<charlie>").out("<follows>").unique().all()
		`,
		expect: []string{"<bob>", "<dani>", "<fred>"},
	},

	// Morphism tests.
	{
		message: "show simple morphism",
		query: `
			grandfollows = g.M().out("<follows>").out("<follows>")
			g.V("<charlie>").follow(grandfollows).all()
		`,
		expect: []string{"<greg>", "<fred>", "<bob>"},
	},
	{
		message: "show reverse morphism",
		query: `
			grandfollows = g.M().out("<follows>").out("<follows>")
			g.V("<fred>").followR(grandfollows).all()
		`,
		expect: []string{"<alice>", "<charlie>", "<dani>"},
	},

	// Intersection tests.
	{
		message: "show simple intersection",
		query: `
			function follows(x) { return g.V(x).out("<follows>") }
			follows("<dani>").and(follows("<charlie>")).all()
		`,
		expect: []string{"<bob>"},
	},
	{
		message: "show simple morphism intersection",
		query: `
			grandfollows = g.M().out("<follows>").out("<follows>")
			function gfollows(x) { return g.V(x).follow(grandfollows) }
			gfollows("<alice>").and(gfollows("<charlie>")).all()
		`,
		expect: []string{"<fred>"},
	},
	{
		message: "show double morphism intersection",
		query: `
			grandfollows = g.M().out("<follows>").out("<follows>")
			function gfollows(x) { return g.V(x).follow(grandfollows) }
			gfollows("<emily>").and(gfollows("<charlie>")).and(gfollows("<bob>")).all()
		`,
		expect: []string{"<greg>"},
	},
	{
		message: "show reverse intersection",
		query: `
			grandfollows = g.M().out("<follows>").out("<follows>")
			g.V("<greg>").followR(grandfollows).intersect(g.V("<fred>").followR(grandfollows)).all()
		`,
		expect: []string{"<charlie>"},
	},
	{
		message: "show standard sort of morphism intersection, continue follow",
		query: `gfollowers = g.M().in("<follows>").in("<follows>")
			function cool(x) { return g.V(x).as("a").out("<status>").is("cool_person").back("a") }
			cool("<greg>").follow(gfollowers).intersect(cool("<bob>").follow(gfollowers)).all()
		`,
		expect: []string{"<charlie>"},
	},
	{
		message: "test Or()",
		query: `
			g.V("<bob>").out("<follows>").or(g.V().has("<status>", "cool_person")).all()
		`,
		expect: []string{"<fred>", "<bob>", "<greg>", "<dani>"},
	},

	// Has tests.
	{
		message: "show a simple Has",
		query: `
				g.V().has("<status>", "cool_person").all()
		`,
		expect: []string{"<greg>", "<dani>", "<bob>"},
	},
	{
		message: "show a simple HasR",
		query: `
				g.V().hasR("<status>", "<bob>").all()
		`,
		expect: []string{"cool_person"},
	},
	{
		message: "show a double Has",
		query: `
				g.V().has("<status>", "cool_person").has("<follows>", "<fred>").all()
		`,
		expect: []string{"<bob>"},
	},
	{
		message: "show a Has with filter",
		query: `
				g.V().has("<follows>", gt("<f>")).all()
		`,
		expect: []string{"<bob>", "<dani>", "<emily>", "<fred>"},
	},

	// Skip/Limit tests.
	{
		message: "use Limit",
		query: `
				g.V().has("<status>", "cool_person").limit(2).all()
		`,
		expect: []string{"<bob>", "<dani>"},
	},
	{
		message: "use Skip",
		query: `
				g.V().has("<status>", "cool_person").skip(2).all()
		`,
		expect: []string{"<greg>"},
	},
	{
		message: "use Skip and Limit",
		query: `
				g.V().has("<status>", "cool_person").skip(1).limit(1).all()
		`,
		expect: []string{"<dani>"},
	},

	{
		message: "show Count",
		query: `
				g.V().has("<status>").count()
		`,
		expect: []string{"5"},
	},
	{
		message: "use Count value",
		query: `
				g.emit(g.V().has("<status>").count()+1)
		`,
		expect: []string{"6"},
	},

	// Tag tests.
	{
		message: "show a simple save",
		query: `
			g.V().save("<status>", "somecool").all()
		`,
		tag:    "somecool",
		expect: []string{"cool_person", "cool_person", "cool_person", "smart_person", "smart_person"},
	},
	{
		message: "show a simple save optional",
		query: `
			g.V("<bob>","<charlie>").out("<follows>").saveOpt("<status>", "somecool").all()
		`,
		tag:    "somecool",
		expect: []string{"cool_person", "cool_person"},
	},
	{
		message: "show a simple saveR",
		query: `
			g.V("cool_person").saveR("<status>", "who").all()
		`,
		tag:    "who",
		expect: []string{"<greg>", "<dani>", "<bob>"},
	},
	{
		message: "show an out save",
		query: `
			g.V("<dani>").out(null, "pred").all()
		`,
		tag:    "pred",
		expect: []string{"<follows>", "<follows>", "<status>"},
	},
	{
		message: "show a tag list",
		query: `
			g.V("<dani>").out(null, ["pred", "foo", "bar"]).all()
		`,
		tag:    "foo",
		expect: []string{"<follows>", "<follows>", "<status>"},
	},
	{
		message: "show a pred list",
		query: `
			g.V("<dani>").out(["<follows>", "<status>"]).all()
		`,
		expect: []string{"<bob>", "<greg>", "cool_person"},
	},
	{
		message: "show a predicate path",
		query: `
			g.V("<dani>").out(g.V("<follows>"), "pred").all()
		`,
		expect: []string{"<bob>", "<greg>"},
	},
	{
		message: "list all bob's incoming predicates",
		query: `
		  g.V("<bob>").inPredicates().all()
		`,
		expect: []string{"<follows>"},
	},
	{
		message: "save all bob's incoming predicates",
		query: `
		  g.V("<bob>").saveInPredicates("pred").all()
		`,
		expect: []string{"<follows>", "<follows>", "<follows>"},
		tag:    "pred",
	},
	{
		message: "list all labels",
		query: `
		  g.V().labels().all()
		`,
		expect: []string{"<smart_graph>"},
	},
	{
		message: "list all in predicates",
		query: `
		  g.V().inPredicates().all()
		`,
		expect: []string{"<are>", "<follows>", "<status>"},
	},
	{
		message: "list all out predicates",
		query: `
		  g.V().outPredicates().all()
		`,
		expect: []string{"<are>", "<follows>", "<status>"},
	},
	{
		message: "traverse using LabelContext",
		query: `
			g.V("<greg>").labelContext("<smart_graph>").out("<status>").all()
		`,
		expect: []string{"smart_person"},
	},
	{
		message: "open and close a LabelContext",
		query: `
			g.V().labelContext("<smart_graph>").in("<status>").labelContext(null).in("<follows>").all()
		`,
		expect: []string{"<dani>", "<fred>"},
	},
	{
		message: "issue #254",
		query:   `g.V({"id":"<alice>"}).all()`,
		expect:  nil, err: true,
	},
	{
		message: "roundtrip values",
		query: `
		v = g.V("<bob>").toValue()
		s = g.V(v).out("<status>").toValue()
		g.V(s).all()
		`,
		expect: []string{"cool_person"},
	},
	{
		message: "roundtrip values (tag map)",
		query: `
		v = g.V("<bob>").tagValue()
		s = g.V(v.id).out("<status>").tagValue()
		g.V(s.id).all()
		`,
		expect: []string{"cool_person"},
	},
	{
		message: "show ToArray",
		query: `
			arr = g.V("<bob>").in("<follows>").toArray()
			for (i in arr) g.emit(arr[i]);
		`,
		expect: []string{"<alice>", "<charlie>", "<dani>"},
	},
	{
		message: "show ToArray with limit",
		query: `
			arr = g.V("<bob>").in("<follows>").toArray(2)
			for (i in arr) g.emit(arr[i]);
		`,
		expect: []string{"<alice>", "<charlie>"},
	},
	{
		message: "show ForEach",
		query: `
			g.V("<bob>").in("<follows>").forEach(function(o){g.emit(o.id)});
		`,
		expect: []string{"<alice>", "<charlie>", "<dani>"},
	},
	{
		message: "show ForEach with limit",
		query: `
			g.V("<bob>").in("<follows>").forEach(2, function(o){g.emit(o.id)});
		`,
		expect: []string{"<alice>", "<charlie>"},
	},
	{
		message: "clone paths",
		query: `
			var alice = g.V('<alice>')
			g.emit(alice.toValue())
			var out = alice.out('<follows>')
			g.emit(out.toValue())
			g.emit(alice.toValue())
		`,
		expect: []string{"<alice>", "<bob>", "<alice>"},
	},
	{
		message: "default namespaces",
		query: `
			g.addDefaultNamespaces()
			g.emit(g.IRI('rdf:type'))
		`,
		expect: []string{"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"},
	},
	{
		message: "add namespace",
		query: `
			g.addNamespace('ex','http://example.net/')
			g.emit(g.IRI('ex:alice'))
		`,
		expect: []string{"<http://example.net/alice>"},
	},
	{
		message: "recursive follow",
		query: `
			g.V("<charlie>").followRecursive("<follows>").all();
		`,
		expect: []string{"<bob>", "<dani>", "<fred>", "<greg>"},
	},
	{
		message: "recursive follow tag",
		query: `
			g.V("<charlie>").followRecursive("<follows>", "depth").all();
		`,
		tag:    "depth",
		expect: []string{intVal(1), intVal(1), intVal(2), intVal(2)},
	},
	{
		message: "recursive follow path",
		query: `
			g.V("<charlie>").followRecursive(g.V().out("<follows>")).all();
		`,
		expect: []string{"<bob>", "<dani>", "<fred>", "<greg>"},
	},
	{
		message: "find non-existent",
		query: `
			g.V('<not-existing>').forEach(function(d){ g.emit(d); })
		`,
		expect: nil,
	},
	{
		message: "default limit All",
		query: `
			g.V().all()
		`,
		limit:  issue718Limit,
		data:   issue718Graph(),
		expect: issue718Nodes(),
	},
	{
		message: "issue #758. Verify saveOpt respects label context",
		query: `
			g.V("<greg>").labelContext("<smart_graph>").saveOpt("<status>", "statusTag").all()
		`,
		tag:    "statusTag",
		file:   multiGraphTestFile,
		expect: []string{"smart_person"},
	},
	{
		message: "issue #758. Verify saveR respects label context.",
		query: `
			g.V("smart_person").labelContext("<other_graph>").saveR("<status>", "who").all()
		`,
		tag:    "who",
		file:   multiGraphTestFile,
		expect: []string{"<fred>"},
	},
}

func runQueryGetTag(rec func(), g []quad.Quad, qu string, tag string, limit int) ([]string, error) {
	js := makeTestSession(g)
	ctx := context.TODO()
	it, err := js.Execute(ctx, qu, query.Options{
		Collation: query.Raw,
		Limit:     limit,
	})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	defer rec()

	var results []string
	for it.Next(ctx) {
		data := it.Result().(*Result)
		if data.Val == nil {
			if val := data.Tags[tag]; val != nil {
				results = append(results, quadValueToString(js.qs.NameOf(val)))
			}
		} else {
			switch v := data.Val.(type) {
			case string:
				results = append(results, v)
			default:
				results = append(results, fmt.Sprint(v))
			}
		}
	}
	if err := it.Err(); err != nil {
		return results, err
	}
	return results, nil
}

func TestGizmo(t *testing.T) {

	simpleGraph := testutil.LoadGraph(t, "../../data/testdata.nq")
	multiGraph := testutil.LoadGraph(t, multiGraphTestFile)

	for _, test := range testQueries {
		test := test
		t.Run(test.message, func(t *testing.T) {
			rec := func() {
				if r := recover(); r != nil {
					t.Errorf("Unexpected panic on %s: %v", test.message, r)
				}
			}
			defer rec()
			if test.tag == "" {
				test.tag = TopResultTag
			}
			quads := simpleGraph
			if test.file == multiGraphTestFile {
				quads = multiGraph
			}

			if test.data != nil {
				quads = test.data
			}
			limit := test.limit
			if limit == 0 {
				limit = -1
			}
			got, err := runQueryGetTag(rec, quads, test.query, test.tag, limit)
			if err != nil {
				if test.err {
					return //expected
				}
				t.Error(err)
			}
			sort.Strings(got)
			sort.Strings(test.expect)
			if !reflect.DeepEqual(got, test.expect) {
				t.Errorf("got: %v expected: %v", got, test.expect)
			}
		})
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
	qu := `g.V().tag('query').out(raw('follows')).out(raw('follows')).forEach(function (item) {
		if (item.id !== item.query) g.emit({ id: item.id });
	})`
	expect := []string{
		"****\nid : alice\n",
		"****\nid : bob\n",
		"****\nid : bob\n",
	}

	ses := makeTestSession(issue160TestGraph)
	ctx := context.TODO()
	it, err := ses.Execute(ctx, qu, query.Options{
		Collation: query.REPL,
		Limit:     100,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	var got []string
	for it.Next(ctx) {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Unexpected panic: %v", r)
				}
			}()
			got = append(got, it.Result().(string))
		}()
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, expect) {
		t.Errorf("Unexpected result, got: %q expected: %q", got, expect)
	}
}

func TestShapeOf(t *testing.T) {
	ses := makeTestSession(nil)
	const query = `g.V().forEach(function(x){
g.emit({id: x.id})
})`
	_, err := ses.ShapeOf(query)
	require.NoError(t, err)
}

const issue718Limit = 5

func issue718Graph() []quad.Quad {
	var quads []quad.Quad
	for i := 0; i < issue718Limit; i++ {
		n := fmt.Sprintf("n%d", i+1)
		quads = append(quads, quad.MakeIRI("a", "b", n, ""))
	}
	return quads
}

func issue718Nodes() []string {
	var nodes []string
	nodes = append(nodes, "<a>", "<b>")
	for i := 0; i < issue718Limit-2; i++ {
		n := fmt.Sprintf("<n%d>", i+1)
		nodes = append(nodes, n)
	}
	return nodes
}
