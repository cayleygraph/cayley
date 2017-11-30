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
	"context"
	"reflect"
	"regexp"
	"sort"
	"testing"
	"time"

	. "github.com/cayleygraph/cayley/graph/path"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest"
	"github.com/cayleygraph/cayley/graph/iterator"
	_ "github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/cayley/quad"
	_ "github.com/cayleygraph/cayley/writer"
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

func makeTestStore(t testing.TB, fnc graphtest.DatabaseFunc, quads ...quad.Quad) (graph.QuadStore, func()) {
	if len(quads) == 0 {
		quads = graphtest.LoadGraph(t, "data/testdata.nq")
	}
	var (
		qs     graph.QuadStore
		opts   graph.Options
		closer = func() {}
	)
	if fnc != nil {
		qs, opts, closer = fnc(t)
	} else {
		qs, _ = graph.NewQuadStore("memstore", "", nil)
	}
	_ = graphtest.MakeWriter(t, qs, opts, quads...)
	return qs, closer
}

func runTopLevel(qs graph.QuadStore, path *Path, opt bool) ([]quad.Value, error) {
	pb := path.Iterate(context.TODO())
	if !opt {
		pb = pb.UnOptimized()
	}
	return pb.Paths(false).AllValues(qs)
}

func runTag(qs graph.QuadStore, path *Path, tag string, opt bool) ([]quad.Value, error) {
	var out []quad.Value
	pb := path.Iterate(context.TODO())
	if !opt {
		pb = pb.UnOptimized()
	}
	err := pb.Paths(true).TagEach(func(tags map[string]graph.Value) {
		if t, ok := tags[tag]; ok {
			out = append(out, qs.NameOf(t))
		}
	})
	return out, err
}

type test struct {
	message   string
	path      *Path
	expect    []quad.Value
	expectAlt [][]quad.Value
	tag       string
}

// Define morphisms without a QuadStore

const (
	vFollows   = quad.IRI("follows")
	vAre       = quad.IRI("are")
	vStatus    = quad.IRI("status")
	vPredicate = quad.IRI("predicates")

	vCool       = quad.String("cool_person")
	vSmart      = quad.String("smart_person")
	vSmartGraph = quad.IRI("smart_graph")

	vAlice   = quad.IRI("alice")
	vBob     = quad.IRI("bob")
	vCharlie = quad.IRI("charlie")
	vDani    = quad.IRI("dani")
	vFred    = quad.IRI("fred")
	vGreg    = quad.IRI("greg")
	vEmily   = quad.IRI("emily")
)

var (
	grandfollows = StartMorphism().Out(vFollows).Out(vFollows)
)

func testSet(qs graph.QuadStore) []test {
	return []test{
		{
			message: "use out",
			path:    StartPath(qs, vAlice).Out(vFollows),
			expect:  []quad.Value{vBob},
		},
		{
			message: "use out (any)",
			path:    StartPath(qs, vBob).Out(),
			expect:  []quad.Value{vFred, vCool},
		},
		{
			message: "use out (raw)",
			path:    StartPath(qs, quad.Raw(vAlice.String())).Out(quad.Raw(vFollows.String())),
			expect:  []quad.Value{vBob},
		},
		{
			message: "use in",
			path:    StartPath(qs, vBob).In(vFollows),
			expect:  []quad.Value{vAlice, vCharlie, vDani},
		},
		{
			message: "use in (any)",
			path:    StartPath(qs, vBob).In(),
			expect:  []quad.Value{vAlice, vCharlie, vDani},
		},
		{
			message: "use in with filter",
			path:    StartPath(qs, vBob).In(vFollows).Filter(iterator.CompareGT, quad.IRI("c")),
			expect:  []quad.Value{vCharlie, vDani},
		},
		{
			message: "use in with regex",
			path:    StartPath(qs, vBob).In(vFollows).Regex(regexp.MustCompile("ar?li.*e")),
			expect:  nil,
		},
		{
			message: "use in with regex (include IRIs)",
			path:    StartPath(qs, vBob).In(vFollows).RegexWithRefs(regexp.MustCompile("ar?li.*e")),
			expect:  []quad.Value{vAlice, vCharlie},
		},
		{
			message: "use path Out",
			path:    StartPath(qs, vBob).Out(StartPath(qs, vPredicate).Out(vAre)),
			expect:  []quad.Value{vFred, vCool},
		},
		{
			message: "use path Out (raw)",
			path:    StartPath(qs, quad.Raw(vBob.String())).Out(StartPath(qs, quad.Raw(vPredicate.String())).Out(quad.Raw(vAre.String()))),
			expect:  []quad.Value{vFred, vCool},
		},
		{
			message: "use And",
			path: StartPath(qs, vDani).Out(vFollows).And(
				StartPath(qs, vCharlie).Out(vFollows)),
			expect: []quad.Value{vBob},
		},
		{
			message: "use Or",
			path: StartPath(qs, vFred).Out(vFollows).Or(
				StartPath(qs, vAlice).Out(vFollows)),
			expect: []quad.Value{vBob, vGreg},
		},
		{
			message: "implicit All",
			path:    StartPath(qs),
			expect:  []quad.Value{vAlice, vBob, vCharlie, vDani, vEmily, vFred, vGreg, vFollows, vStatus, vCool, vPredicate, vAre, vSmartGraph, vSmart},
		},
		{
			message: "follow",
			path:    StartPath(qs, vCharlie).Follow(StartMorphism().Out(vFollows).Out(vFollows)),
			expect:  []quad.Value{vBob, vFred, vGreg},
		},
		{
			message: "followR",
			path:    StartPath(qs, vFred).FollowReverse(StartMorphism().Out(vFollows).Out(vFollows)),
			expect:  []quad.Value{vAlice, vCharlie, vDani},
		},
		{
			message: "is, tag, instead of FollowR",
			path:    StartPath(qs).Tag("first").Follow(StartMorphism().Out(vFollows).Out(vFollows)).Is(vFred),
			expect:  []quad.Value{vAlice, vCharlie, vDani},
			tag:     "first",
		},
		{
			message: "use Except to filter out a single vertex",
			path:    StartPath(qs, vAlice, vBob).Except(StartPath(qs, vAlice)),
			expect:  []quad.Value{vBob},
		},
		{
			message: "use chained Except",
			path:    StartPath(qs, vAlice, vBob, vCharlie).Except(StartPath(qs, vBob)).Except(StartPath(qs, vAlice)),
			expect:  []quad.Value{vCharlie},
		},
		{
			message: "use Unique",
			path:    StartPath(qs, vAlice, vBob, vCharlie).Out(vFollows).Unique(),
			expect:  []quad.Value{vBob, vDani, vFred},
		},
		{
			message: "show a simple save",
			path:    StartPath(qs).Save(vStatus, "somecool"),
			tag:     "somecool",
			expect:  []quad.Value{vCool, vCool, vCool, vSmart, vSmart},
		},
		{
			message: "show a simple saveR",
			path:    StartPath(qs, vCool).SaveReverse(vStatus, "who"),
			tag:     "who",
			expect:  []quad.Value{vGreg, vDani, vBob},
		},
		{
			message: "show a simple Has",
			path:    StartPath(qs).Has(vStatus, vCool),
			expect:  []quad.Value{vGreg, vDani, vBob},
		},
		{
			message: "use Limit",
			path:    StartPath(qs).Has(vStatus, vCool).Limit(2),
			// TODO(dennwc): resolve this ordering issue
			expectAlt: [][]quad.Value{
				{vBob, vGreg},
				{vBob, vDani},
				{vDani, vGreg},
			},
		},
		{
			message: "use Skip",
			path:    StartPath(qs).Has(vStatus, vCool).Skip(2),
			expectAlt: [][]quad.Value{
				{vBob},
				{vDani},
				{vGreg},
			},
		},
		{
			message: "use Skip and Limit",
			path:    StartPath(qs).Has(vStatus, vCool).Skip(1).Limit(1),
			expectAlt: [][]quad.Value{
				{vBob},
				{vDani},
				{vGreg},
			},
		},
		{
			message: "show Count",
			path:    StartPath(qs).Has(vStatus).Count(),
			expect:  []quad.Value{quad.Int(5)},
		},
		{
			message: "show a double Has",
			path:    StartPath(qs).Has(vStatus, vCool).Has(vFollows, vFred),
			expect:  []quad.Value{vBob},
		},
		{
			message: "show a simple HasReverse",
			path:    StartPath(qs).HasReverse(vStatus, vBob),
			expect:  []quad.Value{vCool},
		},
		{
			message: "use .Tag()-.Is()-.Back()",
			path:    StartPath(qs, vBob).In(vFollows).Tag("foo").Out(vStatus).Is(vCool).Back("foo"),
			expect:  []quad.Value{vDani},
		},
		{
			message: "do multiple .Back()s",
			path:    StartPath(qs, vEmily).Out(vFollows).Tag("f").Out(vFollows).Out(vStatus).Is(vCool).Back("f").In(vFollows).In(vFollows).Tag("acd").Out(vStatus).Is(vCool).Back("f"),
			tag:     "acd",
			expect:  []quad.Value{vDani},
		},
		{
			message: "Labels()",
			path:    StartPath(qs, vGreg).Labels(),
			expect:  []quad.Value{vSmartGraph},
		},
		{
			message: "InPredicates()",
			path:    StartPath(qs, vBob).InPredicates(),
			expect:  []quad.Value{vFollows},
		},
		{
			message: "OutPredicates()",
			path:    StartPath(qs, vBob).OutPredicates(),
			expect:  []quad.Value{vFollows, vStatus},
		},
		// Morphism tests
		{
			message: "show simple morphism",
			path:    StartPath(qs, vCharlie).Follow(grandfollows),
			expect:  []quad.Value{vGreg, vFred, vBob},
		},
		{
			message: "show reverse morphism",
			path:    StartPath(qs, vFred).FollowReverse(grandfollows),
			expect:  []quad.Value{vAlice, vCharlie, vDani},
		},
		// Context tests
		{
			message: "query without label limitation",
			path:    StartPath(qs, vGreg).Out(vStatus),
			expect:  []quad.Value{vSmart, vCool},
		},
		{
			message: "query with label limitation",
			path:    StartPath(qs, vGreg).LabelContext(vSmartGraph).Out(vStatus),
			expect:  []quad.Value{vSmart},
		},
		{
			message: "reverse context",
			path:    StartPath(qs, vGreg).Tag("base").LabelContext(vSmartGraph).Out(vStatus).Tag("status").Back("base"),
			expect:  []quad.Value{vGreg},
		},
		// Optional tests
		{
			message: "save limits top level",
			path:    StartPath(qs, vBob, vCharlie).Out(vFollows).Save(vStatus, "statustag"),
			expect:  []quad.Value{vBob, vDani},
		},
		{
			message: "optional still returns top level",
			path:    StartPath(qs, vBob, vCharlie).Out(vFollows).SaveOptional(vStatus, "statustag"),
			expect:  []quad.Value{vBob, vFred, vDani},
		},
		{
			message: "optional has the appropriate tags",
			path:    StartPath(qs, vBob, vCharlie).Out(vFollows).SaveOptional(vStatus, "statustag"),
			tag:     "statustag",
			expect:  []quad.Value{vCool, vCool},
		},
		{
			message: "composite paths (clone paths)",
			path: func() *Path {
				alice_path := StartPath(qs, vAlice)
				_ = alice_path.Out(vFollows)

				return alice_path
			}(),
			expect: []quad.Value{vAlice},
		},
		{
			message: "follow recursive",
			path:    StartPath(qs, vCharlie).FollowRecursive(vFollows, 0, nil),
			expect:  []quad.Value{vBob, vDani, vFred, vGreg},
		},
		{
			message: "find non-existent",
			path:    StartPath(qs, quad.IRI("<not-existing>")),
			expect:  nil,
		},
	}
}

func RunTestMorphisms(t *testing.T, fnc graphtest.DatabaseFunc) {
	for _, ftest := range []func(testing.TB, graphtest.DatabaseFunc){
		testFollowRecursive,
	} {
		ftest(t, fnc)
	}
	qs, closer := makeTestStore(t, fnc)
	defer closer()

	for _, test := range testSet(qs) {
		for _, opt := range []bool{true, false} {
			name := test.message
			if !opt {
				name += " (unoptimized)"
			}
			t.Run(name, func(t *testing.T) {
				var (
					got []quad.Value
					err error
				)
				start := time.Now()
				if test.tag == "" {
					got, err = runTopLevel(qs, test.path, opt)
				} else {
					got, err = runTag(qs, test.path, test.tag, opt)
				}
				dt := time.Since(start)
				if err != nil {
					t.Error(err)
					return
				}
				sort.Sort(quad.ByValueString(got))
				var eq bool
				exp := test.expect
				if test.expectAlt != nil {
					for _, alt := range test.expectAlt {
						exp = alt
						sort.Sort(quad.ByValueString(exp))
						eq = reflect.DeepEqual(got, exp)
						if eq {
							break
						}
					}
				} else {
					sort.Sort(quad.ByValueString(test.expect))
					eq = reflect.DeepEqual(got, test.expect)
				}
				if !eq {
					t.Errorf("got: %v(%d) expected: %v(%d)", got, len(got), exp, len(exp))
				} else {
					t.Logf("%v", dt)
				}
			})
		}
	}
}

func testFollowRecursive(t testing.TB, fnc graphtest.DatabaseFunc) {
	qs, closer := makeTestStore(t, fnc, []quad.Quad{
		quad.MakeIRI("a", "parent", "b", ""),
		quad.MakeIRI("b", "parent", "c", ""),
		quad.MakeIRI("c", "parent", "d", ""),
		quad.MakeIRI("c", "labels", "tag", ""),
		quad.MakeIRI("d", "parent", "e", ""),
		quad.MakeIRI("d", "labels", "tag", ""),
	}...)
	defer closer()

	qu := StartPath(qs, quad.IRI("a")).FollowRecursive(
		StartMorphism().Out(quad.IRI("parent")), 0, nil,
	).Has(quad.IRI("labels"), quad.IRI("tag"))

	expect := []quad.Value{quad.IRI("c"), quad.IRI("d")}

	const msg = "follows recursive order"

	for _, opt := range []bool{true, false} {
		got, err := runTopLevel(qs, qu, opt)
		unopt := ""
		if !opt {
			unopt = " (unoptimized)"
		}
		if err != nil {
			t.Errorf("Failed to check %s%s: %v", msg, unopt, err)
			continue
		}
		sort.Sort(quad.ByValueString(got))
		sort.Sort(quad.ByValueString(expect))
		if !reflect.DeepEqual(got, expect) {
			t.Errorf("Failed to %s%s, got: %v(%d) expected: %v(%d)", msg, unopt, got, len(got), expect, len(expect))
		}
	}
}
