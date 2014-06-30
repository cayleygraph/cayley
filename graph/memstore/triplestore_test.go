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

package memstore

import (
	"sort"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
)

func TestMemstore(t *testing.T) {
	Convey("With a simple memstore", t, func() {
		ts := MakeTestingMemstore()
		Convey("It should have a reasonable size", func() {
			So(ts.Size(), ShouldEqual, 11)
		})
		Convey("It should have an Id Space that makes sense", func() {
			v := ts.GetIdFor("C")
			So(v.(int64), ShouldEqual, 4)
		})
	})
}

func TestIteratorsAndNextResultOrderA(t *testing.T) {
	ts := MakeTestingMemstore()
	fixed := ts.FixedIterator()
	fixed.AddValue(ts.GetIdFor("C"))
	all := ts.GetNodesAllIterator()
	lto := iterator.NewLinksTo(ts, all, graph.Object)
	innerAnd := iterator.NewAnd()

	fixed2 := ts.FixedIterator()
	fixed2.AddValue(ts.GetIdFor("follows"))
	lto2 := iterator.NewLinksTo(ts, fixed2, graph.Predicate)
	innerAnd.AddSubIterator(lto2)
	innerAnd.AddSubIterator(lto)
	hasa := iterator.NewHasA(ts, innerAnd, graph.Subject)
	outerAnd := iterator.NewAnd()
	outerAnd.AddSubIterator(fixed)
	outerAnd.AddSubIterator(hasa)
	val, ok := outerAnd.Next()
	if !ok {
		t.Error("Expected one matching subtree")
	}
	if ts.GetNameFor(val) != "C" {
		t.Errorf("Matching subtree should be %s, got %s", "barak", ts.GetNameFor(val))
	}
	expected := make([]string, 2)
	expected[0] = "B"
	expected[1] = "D"
	actualOut := make([]string, 2)
	actualOut[0] = ts.GetNameFor(all.LastResult())
	nresultOk := outerAnd.NextResult()
	if !nresultOk {
		t.Error("Expected two results got one")
	}
	actualOut[1] = ts.GetNameFor(all.LastResult())
	nresultOk = outerAnd.NextResult()
	if nresultOk {
		t.Error("Expected two results got three")
	}
	CompareStringSlices(t, expected, actualOut)
	val, ok = outerAnd.Next()
	if ok {
		t.Error("More than one possible top level output?")
	}
}

func CompareStringSlices(t *testing.T, expected []string, actual []string) {
	if len(expected) != len(actual) {
		t.Error("String slices are not the same length")
	}
	sort.Strings(expected)
	sort.Strings(actual)
	for i := 0; i < len(expected); i++ {
		if expected[i] != actual[i] {
			t.Errorf("At index %d, expected \"%s\" and got \"%s\"", i, expected[i], actual[i])
		}
	}
}

func TestLinksToOptimization(t *testing.T) {
	ts := MakeTestingMemstore()
	fixed := ts.FixedIterator()
	fixed.AddValue(ts.GetIdFor("cool"))
	lto := iterator.NewLinksTo(ts, fixed, graph.Object)
	lto.AddTag("foo")
	newIt, changed := lto.Optimize()
	if !changed {
		t.Error("Iterator didn't change")
	}
	if newIt.Type() != "llrb" {
		t.Fatal("Didn't swap out to LLRB")
	}
	v := newIt.(*Iterator)
	v_clone := v.Clone()
	if v_clone.DebugString(0) != v.DebugString(0) {
		t.Fatal("Wrong iterator. Got ", v_clone.DebugString(0))
	}
	if len(v_clone.Tags()) < 1 || v_clone.Tags()[0] != "foo" {
		t.Fatal("Tag on LinksTo did not persist")
	}
}

func TestRemoveTriple(t *testing.T) {
	ts := MakeTestingMemstore()
	ts.RemoveTriple(&graph.Triple{"E", "follows", "F", ""})
	fixed := ts.FixedIterator()
	fixed.AddValue(ts.GetIdFor("E"))
	lto := iterator.NewLinksTo(ts, fixed, graph.Subject)
	fixed2 := ts.FixedIterator()
	fixed2.AddValue(ts.GetIdFor("follows"))
	lto2 := iterator.NewLinksTo(ts, fixed2, graph.Predicate)
	innerAnd := iterator.NewAnd()
	innerAnd.AddSubIterator(lto2)
	innerAnd.AddSubIterator(lto)
	hasa := iterator.NewHasA(ts, innerAnd, graph.Object)
	newIt, _ := hasa.Optimize()
	_, ok := newIt.Next()
	if ok {
		t.Error("E should not have any followers.")
	}
}
