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

	"golang.org/x/net/context"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	_ "github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/quad/cquads"
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
	for q1, err := dec.Unmarshal(); err == nil; q1, err = dec.Unmarshal() {
		simpleGraph = append(simpleGraph, q1)
	}
	if err != nil {
		t.Fatalf("Failed to Unmarshal: %v", err)
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

func runTopLevel(path *Path) []quad.Value {
	out, _ := path.Iterate(context.TODO()).Paths(false).AllValues(path.qs)
	return out
}

func runTag(path *Path, tag string) []quad.Value {
	var out []quad.Value
	_ = path.Iterate(context.TODO()).Paths(true).TagEach(func(tags map[string]graph.Value) {
		if t, ok := tags[tag]; ok {
			out = append(out, path.qs.NameOf(t))
		}
	})
	return out
}

type test struct {
	message string
	path    *Path
	expect  []quad.Value
	tag     string
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
			message: "use in with filter",
			path:    StartPath(qs, vBob).In(vFollows).Filter(iterator.CompareGT, quad.IRI("c")),
			expect:  []quad.Value{vCharlie, vDani},
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
			expect:  []quad.Value{vBob, vDani},
		},
		{
			message: "use Skip",
			path:    StartPath(qs).Has(vStatus, vCool).Skip(2),
			expect:  []quad.Value{vGreg},
		},
		{
			message: "use Skip and Limit",
			path:    StartPath(qs).Has(vStatus, vCool).Skip(1).Limit(1),
			expect:  []quad.Value{vDani},
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
	}
}

func TestMorphisms(t *testing.T) {
	qs := makeTestStore(t)
	for _, test := range testSet(qs) {
		var got []quad.Value
		if test.tag == "" {
			got = runTopLevel(test.path)
		} else {
			got = runTag(test.path, test.tag)
		}
		sort.Sort(quad.ByValueString(got))
		sort.Sort(quad.ByValueString(test.expect))
		if !reflect.DeepEqual(got, test.expect) {
			t.Errorf("Failed to %s, got: %v(%d) expected: %v(%d)", test.message, got, len(got), test.expect, len(test.expect))
		}
	}
}
