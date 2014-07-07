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

package leveldb

import (
	"io/ioutil"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
)

func makeTripleSet() []*graph.Triple {
	tripleSet := []*graph.Triple{
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
	return tripleSet
}

func iteratedTriples(ts graph.TripleStore, it graph.Iterator) []*graph.Triple {
	var res ordered
	for {
		val, ok := it.Next()
		if !ok {
			break
		}
		res = append(res, ts.Triple(val))
	}
	sort.Sort(res)
	return res
}

type ordered []*graph.Triple

func (o ordered) Len() int { return len(o) }
func (o ordered) Less(i, j int) bool {
	switch {
	case o[i].Subject < o[j].Subject,

		o[i].Subject == o[j].Subject &&
			o[i].Predicate < o[j].Predicate,

		o[i].Subject == o[j].Subject &&
			o[i].Predicate == o[j].Predicate &&
			o[i].Object < o[j].Object,

		o[i].Subject == o[j].Subject &&
			o[i].Predicate == o[j].Predicate &&
			o[i].Object == o[j].Object &&
			o[i].Provenance < o[j].Provenance:

		return true

	default:
		return false
	}
}
func (o ordered) Swap(i, j int) { o[i], o[j] = o[j], o[i] }

func iteratedNames(ts graph.TripleStore, it graph.Iterator) []string {
	var res []string
	for {
		val, ok := it.Next()
		if !ok {
			break
		}
		res = append(res, ts.NameOf(val))
	}
	sort.Strings(res)
	return res
}

func TestCreateDatabase(t *testing.T) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "cayley_test")
	if err != nil {
		t.Fatalf("Could not create working directory: %v", err)
	}
	t.Log(tmpDir)

	if created := CreateNewLevelDB(tmpDir); !created {
		t.Fatal("Failed to create LevelDB database.")
	}

	ts := NewTripleStore(tmpDir, nil)
	if ts == nil {
		t.Error("Failed to create leveldb TripleStore.")
	}
	if s := ts.Size(); s != 0 {
		t.Errorf("Unexpected size, got:%d expected:0", s)
	}
	ts.Close()

	if created := CreateNewLevelDB("/dev/null/some terrible path"); created {
		t.Errorf("Created LevelDB database for bad path.")
	}
	// TODO(kortschak) Invalidate this test by using error returns rather than panics.
	var panicked bool
	func() {
		defer func() {
			r := recover()
			panicked = r != nil
		}()
		NewTripleStore("/dev/null/some terrible path", nil)
	}()
	if !panicked {
		t.Error("NewTripleStore failed to panic with bad path.")
	}

	os.RemoveAll(tmpDir)
}

func TestLoadDatabase(t *testing.T) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "cayley_test")
	if err != nil {
		t.Fatalf("Could not create working directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	t.Log(tmpDir)

	if created := CreateNewLevelDB(tmpDir); !created {
		t.Fatal("Failed to create LevelDB database.")
	}

	var ts *TripleStore

	ts = NewTripleStore(tmpDir, nil)
	if ts == nil {
		t.Error("Failed to create leveldb TripleStore.")
	}

	ts.AddTriple(&graph.Triple{"Something", "points_to", "Something Else", "context"})
	for _, pq := range []string{"Something", "points_to", "Something Else", "context"} {
		if got := ts.NameOf(ts.ValueOf(pq)); got != pq {
			t.Errorf("Failed to roundtrip %q, got:%q expect:%q", pq, got, pq)
		}
	}
	if s := ts.Size(); s != 1 {
		t.Errorf("Unexpected triplestore size, got:%d expect:1", s)
	}
	ts.Close()

	if created := CreateNewLevelDB(tmpDir); !created {
		t.Fatal("Failed to create LevelDB database.")
	}
	ts = NewTripleStore(tmpDir, nil)
	if ts == nil {
		t.Error("Failed to create leveldb TripleStore.")
	}

	ts.AddTripleSet(makeTripleSet())
	if s := ts.Size(); s != 11 {
		t.Errorf("Unexpected triplestore size, got:%d expect:11", s)
	}
	if s := ts.SizeOf(ts.ValueOf("B")); s != 5 {
		t.Errorf("Unexpected triplestore size, got:%d expect:5", s)
	}

	ts.RemoveTriple(&graph.Triple{"A", "follows", "B", ""})
	if s := ts.Size(); s != 10 {
		t.Errorf("Unexpected triplestore size after RemoveTriple, got:%d expect:10", s)
	}
	if s := ts.SizeOf(ts.ValueOf("B")); s != 4 {
		t.Errorf("Unexpected triplestore size, got:%d expect:4", s)
	}

	ts.Close()
}

func TestIterator(t *testing.T) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "cayley_test")
	if err != nil {
		t.Fatalf("Could not create working directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	t.Log(tmpDir)

	if created := CreateNewLevelDB(tmpDir); !created {
		t.Fatal("Failed to create LevelDB database.")
	}

	var ts *TripleStore
	ts = NewTripleStore(tmpDir, nil)
	ts.AddTripleSet(makeTripleSet())
	var it graph.Iterator

	it = ts.NodesAllIterator()
	if it == nil {
		t.Fatal("Got nil iterator.")
	}

	size, exact := it.Size()
	if size <= 0 || size >= 20 {
		t.Errorf("Unexpected size, got:%d expect:(0, 20)", size)
	}
	if exact {
		t.Errorf("Got unexpected exact result.")
	}
	if typ := it.Type(); typ != graph.All {
		t.Errorf("Unexpected iterator type, got:%v expect:%v", typ, graph.All)
	}
	optIt, changed := it.Optimize()
	if changed || optIt != it {
		t.Errorf("Optimize unexpectedly changed iterator.")
	}

	expect := []string{
		"A",
		"B",
		"C",
		"D",
		"E",
		"F",
		"G",
		"follows",
		"status",
		"cool",
		"status_graph",
	}
	sort.Strings(expect)
	for i := 0; i < 2; i++ {
		got := iteratedNames(ts, it)
		sort.Strings(got)
		if !reflect.DeepEqual(got, expect) {
			t.Errorf("Unexpected iterated result on repeat %d, got:%v expect:%v", i, got, expect)
		}
		it.Reset()
	}

	for _, pq := range expect {
		if !it.Check(ts.ValueOf(pq)) {
			t.Errorf("Failed to find and check %q correctly", pq)
		}
	}
	// FIXME(kortschak) Why does this fail?
	/*
		for _, pq := range []string{"baller"} {
			if it.Check(ts.ValueOf(pq)) {
				t.Errorf("Failed to check %q correctly", pq)
			}
		}
	*/
	it.Reset()

	it = ts.TriplesAllIterator()
	edge, _ := it.Next()
	triple := ts.Triple(edge)
	set := makeTripleSet()
	var ok bool
	for _, t := range set {
		if t.String() == triple.String() {
			ok = true
			break
		}
	}
	if !ok {
		t.Errorf("Failed to find %q during iteration, got:%q", triple, set)
	}

	ts.Close()
}

func TestSetIterator(t *testing.T) {

	tmpDir, _ := ioutil.TempDir(os.TempDir(), "cayley_test")
	t.Log(tmpDir)
	defer os.RemoveAll(tmpDir)
	ok := CreateNewLevelDB(tmpDir)
	if !ok {
		t.Fatalf("Failed to create working directory")
	}

	ts := NewTripleStore(tmpDir, nil)
	defer ts.Close()

	ts.AddTripleSet(makeTripleSet())

	expect := []*graph.Triple{
		{"C", "follows", "B", ""},
		{"C", "follows", "D", ""},
	}
	sort.Sort(ordered(expect))

	// Subject iterator.
	it := ts.TripleIterator(graph.Subject, ts.ValueOf("C"))

	if got := iteratedTriples(ts, it); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get expected results, got:%v expect:%v", got, expect)
	}
	it.Reset()

	and := iterator.NewAnd()
	and.AddSubIterator(ts.TriplesAllIterator())
	and.AddSubIterator(it)

	if got := iteratedTriples(ts, and); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get confirm expected results, got:%v expect:%v", got, expect)
	}

	// Object iterator.
	it = ts.TripleIterator(graph.Object, ts.ValueOf("F"))

	expect = []*graph.Triple{
		{"B", "follows", "F", ""},
		{"E", "follows", "F", ""},
	}
	sort.Sort(ordered(expect))
	if got := iteratedTriples(ts, it); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get expected results, got:%v expect:%v", got, expect)
	}

	and = iterator.NewAnd()
	and.AddSubIterator(ts.TripleIterator(graph.Subject, ts.ValueOf("B")))
	and.AddSubIterator(it)

	expect = []*graph.Triple{
		{"B", "follows", "F", ""},
	}
	if got := iteratedTriples(ts, and); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get confirm expected results, got:%v expect:%v", got, expect)
	}

	// Predicate iterator.
	it = ts.TripleIterator(graph.Predicate, ts.ValueOf("status"))

	expect = []*graph.Triple{
		{"B", "status", "cool", "status_graph"},
		{"D", "status", "cool", "status_graph"},
		{"G", "status", "cool", "status_graph"},
	}
	sort.Sort(ordered(expect))
	if got := iteratedTriples(ts, it); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get expected results from predicate iterator, got:%v expect:%v", got, expect)
	}

	// Provenance iterator.
	it = ts.TripleIterator(graph.Provenance, ts.ValueOf("status_graph"))

	expect = []*graph.Triple{
		{"B", "status", "cool", "status_graph"},
		{"D", "status", "cool", "status_graph"},
		{"G", "status", "cool", "status_graph"},
	}
	sort.Sort(ordered(expect))
	if got := iteratedTriples(ts, it); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get expected results from predicate iterator, got:%v expect:%v", got, expect)
	}
	it.Reset()

	// Order is important
	and = iterator.NewAnd()
	and.AddSubIterator(ts.TripleIterator(graph.Subject, ts.ValueOf("B")))
	and.AddSubIterator(it)

	expect = []*graph.Triple{
		{"B", "status", "cool", "status_graph"},
	}
	if got := iteratedTriples(ts, and); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get confirm expected results, got:%v expect:%v", got, expect)
	}
	it.Reset()

	// Order is important
	and = iterator.NewAnd()
	and.AddSubIterator(it)
	and.AddSubIterator(ts.TripleIterator(graph.Subject, ts.ValueOf("B")))

	expect = []*graph.Triple{
		{"B", "status", "cool", "status_graph"},
	}
	if got := iteratedTriples(ts, and); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get confirm expected results, got:%v expect:%v", got, expect)
	}
}

func TestOptimize(t *testing.T) {
	tmpDir, _ := ioutil.TempDir(os.TempDir(), "cayley_test")
	t.Log(tmpDir)
	defer os.RemoveAll(tmpDir)
	ok := CreateNewLevelDB(tmpDir)
	if !ok {
		t.Fatalf("Failed to create working directory")
	}
	ts := NewTripleStore(tmpDir, nil)
	ts.AddTripleSet(makeTripleSet())

	// With an linksto-fixed pair
	fixed := ts.FixedIterator()
	fixed.Add(ts.ValueOf("F"))
	fixed.AddTag("internal")
	lto := iterator.NewLinksTo(ts, fixed, graph.Object)

	oldIt := lto.Clone()
	newIt, ok := lto.Optimize()
	if !ok {
		t.Errorf("Failed to optimize iterator")
	}
	if newIt.Type() != Type() {
		t.Errorf("Optimized iterator type does not match original, got:%v expect:%v", newIt.Type(), Type())
	}

	newTriples := iteratedTriples(ts, newIt)
	oldTriples := iteratedTriples(ts, oldIt)
	if !reflect.DeepEqual(newTriples, oldTriples) {
		t.Errorf("Optimized iteration does not match original")
	}

	oldIt.Next()
	oldResults := make(map[string]graph.Value)
	oldIt.TagResults(oldResults)
	newIt.Next()
	newResults := make(map[string]graph.Value)
	newIt.TagResults(newResults)
	if !reflect.DeepEqual(newResults, oldResults) {
		t.Errorf("Discordant tag results, new:%v old:%v", newResults, oldResults)
	}
}
