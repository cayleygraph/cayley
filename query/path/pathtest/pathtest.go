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

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest/testutil"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/cayley/query/shape"
	_ "github.com/cayleygraph/cayley/writer"
	"github.com/cayleygraph/quad"
	"github.com/stretchr/testify/require"
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

func makeTestStore(t testing.TB, fnc testutil.DatabaseFunc, quads ...quad.Quad) (graph.QuadStore, func()) {
	if len(quads) == 0 {
		quads = testutil.LoadGraph(t, "data/testdata.nq")
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
	_ = testutil.MakeWriter(t, qs, opts, quads...)
	return qs, closer
}

func runTopLevel(qs graph.QuadStore, path *path.Path, opt bool) ([]quad.Value, error) {
	pb := path.Iterate(context.TODO())
	if !opt {
		pb = pb.UnOptimized()
	}
	return pb.Paths(false).AllValues(qs)
}

func runTag(qs graph.QuadStore, path *path.Path, tag string, opt, keepEmpty bool) ([]quad.Value, error) {
	var out []quad.Value
	pb := path.Iterate(context.TODO())
	if !opt {
		pb = pb.UnOptimized()
	}
	err := pb.Paths(true).TagEach(func(tags map[string]graph.Ref) {
		if t, ok := tags[tag]; ok {
			out = append(out, qs.NameOf(t))
		} else if keepEmpty {
			out = append(out, vEmpty)
		}
	})
	return out, err
}

func runAllTags(qs graph.QuadStore, path *path.Path, opt bool) ([]map[string]quad.Value, error) {
	var out []map[string]quad.Value
	pb := path.Iterate(context.TODO())
	if !opt {
		pb = pb.UnOptimized()
	}
	err := pb.Paths(true).TagValues(qs, func(tags map[string]quad.Value) {
		out = append(out, tags)
	})
	return out, err
}

type test struct {
	skip      bool
	message   string
	path      *path.Path
	expect    []quad.Value
	expectAlt [][]quad.Value
	tag       string
	unsorted  bool
	empty     bool // do not skip empty tags
}

// Define morphisms without a QuadStore

const (
	vEmpty = quad.String("")

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
	grandfollows = path.StartMorphism().Out(vFollows).Out(vFollows)
)

func testSet(qs graph.QuadStore) []test {
	return []test{
		{
			message: "out",
			path:    path.StartPath(qs, vAlice).Out(vFollows),
			expect:  []quad.Value{vBob},
		},
		{
			message: "out (any)",
			path:    path.StartPath(qs, vBob).Out(),
			expect:  []quad.Value{vFred, vCool},
		},
		{
			message: "out (raw)",
			path:    path.StartPath(qs, quad.Raw(vAlice.String())).Out(quad.Raw(vFollows.String())),
			expect:  []quad.Value{vBob},
		},
		{
			message: "in",
			path:    path.StartPath(qs, vBob).In(vFollows),
			expect:  []quad.Value{vAlice, vCharlie, vDani},
		},
		{
			message: "in (any)",
			path:    path.StartPath(qs, vBob).In(),
			expect:  []quad.Value{vAlice, vCharlie, vDani},
		},
		{
			message: "filter nodes",
			path:    path.StartPath(qs).Filter(iterator.CompareGT, quad.IRI("p")),
			expect:  []quad.Value{vPredicate, vSmartGraph, vStatus},
		},
		{
			message: "in with filter",
			path:    path.StartPath(qs, vBob).In(vFollows).Filter(iterator.CompareGT, quad.IRI("c")),
			expect:  []quad.Value{vCharlie, vDani},
		},
		{
			message: "in with regex",
			path:    path.StartPath(qs, vBob).In(vFollows).Regex(regexp.MustCompile("ar?li.*e")),
			expect:  nil,
		},
		{
			message: "in with regex (include IRIs)",
			path:    path.StartPath(qs, vBob).In(vFollows).RegexWithRefs(regexp.MustCompile("ar?li.*e")),
			expect:  []quad.Value{vAlice, vCharlie},
		},
		{
			message: "path Out",
			path:    path.StartPath(qs, vBob).Out(path.StartPath(qs, vPredicate).Out(vAre)),
			expect:  []quad.Value{vFred, vCool},
		},
		{
			message: "path Out (raw)",
			path:    path.StartPath(qs, quad.Raw(vBob.String())).Out(path.StartPath(qs, quad.Raw(vPredicate.String())).Out(quad.Raw(vAre.String()))),
			expect:  []quad.Value{vFred, vCool},
		},
		{
			message: "And",
			path: path.StartPath(qs, vDani).Out(vFollows).And(
				path.StartPath(qs, vCharlie).Out(vFollows)),
			expect: []quad.Value{vBob},
		},
		{
			message: "Or",
			path: path.StartPath(qs, vFred).Out(vFollows).Or(
				path.StartPath(qs, vAlice).Out(vFollows)),
			expect: []quad.Value{vBob, vGreg},
		},
		{
			message: "implicit All",
			path:    path.StartPath(qs),
			expect:  []quad.Value{vAlice, vBob, vCharlie, vDani, vEmily, vFred, vGreg, vFollows, vStatus, vCool, vPredicate, vAre, vSmartGraph, vSmart},
		},
		{
			message: "follow",
			path:    path.StartPath(qs, vCharlie).Follow(path.StartMorphism().Out(vFollows).Out(vFollows)),
			expect:  []quad.Value{vBob, vFred, vGreg},
		},
		{
			message: "followR",
			path:    path.StartPath(qs, vFred).FollowReverse(path.StartMorphism().Out(vFollows).Out(vFollows)),
			expect:  []quad.Value{vAlice, vCharlie, vDani},
		},
		{
			message: "is, tag, instead of FollowR",
			path:    path.StartPath(qs).Tag("first").Follow(path.StartMorphism().Out(vFollows).Out(vFollows)).Is(vFred),
			expect:  []quad.Value{vAlice, vCharlie, vDani},
			tag:     "first",
		},
		{
			message: "Except to filter out a single vertex",
			path:    path.StartPath(qs, vAlice, vBob).Except(path.StartPath(qs, vAlice)),
			expect:  []quad.Value{vBob},
		},
		{
			message: "chained Except",
			path:    path.StartPath(qs, vAlice, vBob, vCharlie).Except(path.StartPath(qs, vBob)).Except(path.StartPath(qs, vAlice)),
			expect:  []quad.Value{vCharlie},
		},
		{
			message: "Unique",
			path:    path.StartPath(qs, vAlice, vBob, vCharlie).Out(vFollows).Unique(),
			expect:  []quad.Value{vBob, vDani, vFred},
		},
		{
			message: "simple save",
			path:    path.StartPath(qs).Save(vStatus, "somecool"),
			tag:     "somecool",
			expect:  []quad.Value{vCool, vCool, vCool, vSmart, vSmart},
		},
		{
			message: "simple saveR",
			path:    path.StartPath(qs, vCool).SaveReverse(vStatus, "who"),
			tag:     "who",
			expect:  []quad.Value{vGreg, vDani, vBob},
		},
		{
			message: "save with a next path",
			path:    path.StartPath(qs, vDani, vBob).Save(vFollows, "target"),
			tag:     "target",
			expect:  []quad.Value{vBob, vFred, vGreg},
		},
		{
			message: "save all with a next path",
			path:    path.StartPath(qs).Save(vFollows, "target"),
			tag:     "target",
			expect:  []quad.Value{vBob, vBob, vBob, vDani, vFred, vFred, vGreg, vGreg},
		},
		{
			message: "simple Has",
			path:    path.StartPath(qs).Has(vStatus, vCool),
			expect:  []quad.Value{vGreg, vDani, vBob},
		},
		{
			message: "filter nodes with has",
			path: path.StartPath(qs).HasFilter(vFollows, false, shape.Comparison{
				Op: iterator.CompareGT, Val: quad.IRI("f"),
			}),
			expect: []quad.Value{vBob, vDani, vEmily, vFred},
		},
		{
			message: "has path",
			path:    path.StartPath(qs).HasPath(path.StartMorphism().Out(vStatus).Is(vCool)),
			expect:  []quad.Value{vGreg, vDani, vBob},
		},
		{
			message: "string prefix",
			path: path.StartPath(qs).Filters(shape.Wildcard{
				Pattern: `bo%`,
			}),
			expect: []quad.Value{vBob},
		},
		{
			message: "three letters and range",
			path: path.StartPath(qs).Filters(shape.Wildcard{
				Pattern: `???`,
			}, shape.Comparison{
				Op: iterator.CompareGT, Val: quad.IRI("b"),
			}),
			expect: []quad.Value{vBob},
		},
		{
			message: "part in string",
			path: path.StartPath(qs).Filters(shape.Wildcard{
				Pattern: `%ed%`,
			}),
			expect: []quad.Value{vFred, vPredicate},
		},
		{
			message: "Limit",
			path:    path.StartPath(qs).Has(vStatus, vCool).Limit(2),
			// TODO(dennwc): resolve this ordering issue
			expectAlt: [][]quad.Value{
				{vBob, vGreg},
				{vBob, vDani},
				{vDani, vGreg},
			},
		},
		{
			message: "Skip",
			path:    path.StartPath(qs).Has(vStatus, vCool).Skip(2),
			expectAlt: [][]quad.Value{
				{vBob},
				{vDani},
				{vGreg},
			},
		},
		{
			message: "Skip and Limit",
			path:    path.StartPath(qs).Has(vStatus, vCool).Skip(1).Limit(1),
			expectAlt: [][]quad.Value{
				{vBob},
				{vDani},
				{vGreg},
			},
		},
		{
			message: "Count",
			path:    path.StartPath(qs).Has(vStatus).Count(),
			expect:  []quad.Value{quad.Int(5)},
		},
		{
			message: "double Has",
			path:    path.StartPath(qs).Has(vStatus, vCool).Has(vFollows, vFred),
			expect:  []quad.Value{vBob},
		},
		{
			message: "simple HasReverse",
			path:    path.StartPath(qs).HasReverse(vStatus, vBob),
			expect:  []quad.Value{vCool},
		},
		{
			message: ".Tag()-.Is()-.Back()",
			path:    path.StartPath(qs, vBob).In(vFollows).Tag("foo").Out(vStatus).Is(vCool).Back("foo"),
			expect:  []quad.Value{vDani},
		},
		{
			message: "do multiple .Back()s",
			path:    path.StartPath(qs, vEmily).Out(vFollows).Tag("f").Out(vFollows).Out(vStatus).Is(vCool).Back("f").In(vFollows).In(vFollows).Tag("acd").Out(vStatus).Is(vCool).Back("f"),
			tag:     "acd",
			expect:  []quad.Value{vDani},
		},
		{
			message: "Labels()",
			path:    path.StartPath(qs, vGreg).Labels(),
			expect:  []quad.Value{vSmartGraph},
		},
		{
			message: "InPredicates()",
			path:    path.StartPath(qs, vBob).InPredicates(),
			expect:  []quad.Value{vFollows},
		},
		{
			message: "OutPredicates()",
			path:    path.StartPath(qs, vBob).OutPredicates(),
			expect:  []quad.Value{vFollows, vStatus},
		},
		{
			message: "SavePredicates(in)",
			path:    path.StartPath(qs, vBob).SavePredicates(true, "pred"),
			expect:  []quad.Value{vFollows, vFollows, vFollows},
			tag:     "pred",
		},
		{
			message: "SavePredicates(out)",
			path:    path.StartPath(qs, vBob).SavePredicates(false, "pred"),
			expect:  []quad.Value{vFollows, vStatus},
			tag:     "pred",
		},
		// Morphism tests
		{
			message: "simple morphism",
			path:    path.StartPath(qs, vCharlie).Follow(grandfollows),
			expect:  []quad.Value{vGreg, vFred, vBob},
		},
		{
			message: "reverse morphism",
			path:    path.StartPath(qs, vFred).FollowReverse(grandfollows),
			expect:  []quad.Value{vAlice, vCharlie, vDani},
		},
		// Context tests
		{
			message: "query without label limitation",
			path:    path.StartPath(qs, vGreg).Out(vStatus),
			expect:  []quad.Value{vSmart, vCool},
		},
		{
			message: "query with label limitation",
			path:    path.StartPath(qs, vGreg).LabelContext(vSmartGraph).Out(vStatus),
			expect:  []quad.Value{vSmart},
		},
		{
			message: "reverse context",
			path:    path.StartPath(qs, vGreg).Tag("base").LabelContext(vSmartGraph).Out(vStatus).Tag("status").Back("base"),
			expect:  []quad.Value{vGreg},
		},
		// Optional tests
		{
			message: "save limits top level",
			path:    path.StartPath(qs, vBob, vCharlie).Out(vFollows).Save(vStatus, "statustag"),
			expect:  []quad.Value{vBob, vDani},
		},
		{
			message: "optional still returns top level",
			path:    path.StartPath(qs, vBob, vCharlie).Out(vFollows).SaveOptional(vStatus, "statustag"),
			expect:  []quad.Value{vBob, vFred, vDani},
		},
		{
			message: "optional has the appropriate tags",
			path:    path.StartPath(qs, vBob, vCharlie).Out(vFollows).SaveOptional(vStatus, "statustag"),
			tag:     "statustag",
			expect:  []quad.Value{vCool, vCool},
		},
		{
			message: "composite paths (clone paths)",
			path: func() *path.Path {
				alicePath := path.StartPath(qs, vAlice)
				_ = alicePath.Out(vFollows)

				return alicePath
			}(),
			expect: []quad.Value{vAlice},
		},
		{
			message: "follow recursive",
			path:    path.StartPath(qs, vCharlie).FollowRecursive(vFollows, 0, nil),
			expect:  []quad.Value{vBob, vDani, vFred, vGreg},
		},
		{
			message: "follow recursive (limit depth)",
			path:    path.StartPath(qs, vCharlie).FollowRecursive(vFollows, 1, nil),
			expect:  []quad.Value{vBob, vDani},
		},
		{
			message: "find non-existent",
			path:    path.StartPath(qs, quad.IRI("<not-existing>")),
			expect:  nil,
		},
		{
			message: "use order",
			path:    path.StartPath(qs).Order(),
			expect: []quad.Value{
				vAlice,
				vAre,
				vBob,
				vCharlie,
				vDani,
				vEmily,
				vFollows,
				vFred,
				vGreg,
				vPredicate,
				vSmartGraph,
				vStatus,
				vCool,
				vSmart,
			},
		},
		{
			message: "use order tags",
			path:    path.StartPath(qs).Tag("target").Order(),
			tag:     "target",
			expect: []quad.Value{
				vAlice,
				vAre,
				vBob,
				vCharlie,
				vDani,
				vEmily,
				vFollows,
				vFred,
				vGreg,
				vPredicate,
				vSmartGraph,
				vStatus,
				vCool,
				vSmart,
			},
		},
		{
			message: "order with a next path",
			path:    path.StartPath(qs, vDani, vBob).Save(vFollows, "target").Order(),
			tag:     "target",
			expect:  []quad.Value{vBob, vFred, vGreg},
		},
		{
			message:  "order with a next path",
			path:     path.StartPath(qs).Order().Has(vFollows, vBob),
			expect:   []quad.Value{vAlice, vCharlie, vDani},
			unsorted: true,
			skip:     true, // TODO(dennwc): optimize Order in And properly
		},
		{
			message: "optional path",
			path:    path.StartPath(qs, vBob, vDani, vFred).Optional(path.StartMorphism().Save(vStatus, "status")),
			tag:     "status",
			empty:   true,
			expect:  []quad.Value{vEmpty, vCool, vCool},
		},
	}
}

func RunTestMorphisms(t *testing.T, fnc testutil.DatabaseFunc) {
	for _, ftest := range []func(*testing.T, testutil.DatabaseFunc){
		testFollowRecursive,
		testFollowRecursiveHas,
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
				if test.skip {
					t.SkipNow()
				}
				var (
					got []quad.Value
					err error
				)
				start := time.Now()
				if test.tag == "" {
					got, err = runTopLevel(qs, test.path, opt)
				} else {
					got, err = runTag(qs, test.path, test.tag, opt, test.empty)
				}
				dt := time.Since(start)
				if err != nil {
					t.Error(err)
					return
				}
				if !test.unsorted {
					sort.Sort(quad.ByValueString(got))
				}
				var eq bool
				exp := test.expect
				if test.expectAlt != nil {
					for _, alt := range test.expectAlt {
						exp = alt
						if !test.unsorted {
							sort.Sort(quad.ByValueString(exp))
						}
						eq = reflect.DeepEqual(got, exp)
						if eq {
							break
						}
					}
				} else {
					if !test.unsorted {
						sort.Sort(quad.ByValueString(test.expect))
					}
					eq = reflect.DeepEqual(got, test.expect)
				}
				if !eq {
					t.Errorf("got: %v(%d) expected: %v(%d)", got, len(got), exp, len(exp))
				} else {
					t.Logf("%12v %v", dt, name)
				}
			})
		}
	}
}

func testFollowRecursive(t *testing.T, fnc testutil.DatabaseFunc) {
	qs, closer := makeTestStore(t, fnc, []quad.Quad{
		quad.MakeIRI("a", "parent", "b", ""),
		quad.MakeIRI("b", "parent", "c", ""),
		quad.MakeIRI("c", "parent", "d", ""),
		quad.MakeIRI("c", "labels", "tag", ""),
		quad.MakeIRI("d", "parent", "e", ""),
		quad.MakeIRI("d", "labels", "tag", ""),
	}...)
	defer closer()

	qu := path.StartPath(qs, quad.IRI("a")).FollowRecursive(
		path.StartMorphism().Out(quad.IRI("parent")), 0, nil,
	).Has(quad.IRI("labels"), quad.IRI("tag"))

	expect := []quad.Value{quad.IRI("c"), quad.IRI("d")}

	const msg = "follows recursive order"

	for _, opt := range []bool{true, false} {
		unopt := ""
		if !opt {
			unopt = " (unoptimized)"
		}
		t.Run(msg+unopt, func(t *testing.T) {
			got, err := runTopLevel(qs, qu, opt)
			if err != nil {
				t.Errorf("Failed to check %s%s: %v", msg, unopt, err)
				return
			}
			sort.Sort(quad.ByValueString(got))
			sort.Sort(quad.ByValueString(expect))
			if !reflect.DeepEqual(got, expect) {
				t.Errorf("Failed to %s%s, got: %v(%d) expected: %v(%d)", msg, unopt, got, len(got), expect, len(expect))
			}
		})
	}
}

type byTags struct {
	tags []string
	arr  []map[string]quad.Value
}

func (b byTags) Len() int {
	return len(b.arr)
}

func (b byTags) Less(i, j int) bool {
	m1, m2 := b.arr[i], b.arr[j]
	for _, t := range b.tags {
		v1, v2 := m1[t], m2[t]
		s1, s2 := quad.ToString(v1), quad.ToString(v2)
		if s1 < s2 {
			return true
		} else if s1 > s2 {
			return false
		}
	}
	return false
}

func (b byTags) Swap(i, j int) {
	b.arr[i], b.arr[j] = b.arr[j], b.arr[i]
}

func testFollowRecursiveHas(t *testing.T, fnc testutil.DatabaseFunc) {
	qs, closer := makeTestStore(t, fnc, []quad.Quad{
		quad.MakeIRI("1", "relatesTo", "x", ""),
		quad.MakeIRI("2", "relatesTo", "x", ""),
		quad.MakeIRI("3", "relatesTo", "y", ""),
		quad.MakeIRI("1", "knows", "2", ""),
		quad.MakeIRI("2", "knows", "3", ""),
		quad.MakeIRI("2", "knows", "1", ""),
	}...)
	defer closer()

	qu := path.StartPath(qs, quad.IRI("1")).FollowRecursive(
		path.StartMorphism().Tag("pid").Out(quad.IRI("knows")), 2, nil,
	).Has(quad.IRI("relatesTo")).Tag("id")

	expect := []map[string]quad.Value{
		{"id": quad.IRI("1"), "pid": quad.IRI("2")},
		{"id": quad.IRI("2"), "pid": quad.IRI("1")},
		{"id": quad.IRI("3"), "pid": quad.IRI("2")},
	}
	sortTags := []string{"id", "pid"}
	sort.Sort(byTags{
		tags: sortTags,
		arr:  expect,
	})

	const msg = "follows recursive loop"

	for _, opt := range []bool{true, false} {
		unopt := ""
		if !opt {
			unopt = " (unoptimized)"
		}
		t.Run(msg+unopt, func(t *testing.T) {
			got, err := runAllTags(qs, qu, opt)
			if err != nil {
				t.Errorf("Failed to check %s%s: %v", msg, unopt, err)
				return
			}
			sort.Sort(byTags{
				tags: sortTags,
				arr:  got,
			})
			require.Equal(t, expect, got)
		})
	}
}
