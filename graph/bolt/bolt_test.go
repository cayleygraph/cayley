// Copyright 2015 The Cayley Authors. All rights reserved.
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

package bolt

import (
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
	"github.com/google/cayley/writer"
)

func makeQuadSet() []quad.Quad {
	quadSet := []quad.Quad{
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
	return quadSet
}

func iteratedQuads(qs graph.QuadStore, it graph.Iterator) []quad.Quad {
	var res ordered
	for graph.Next(it) {
		res = append(res, qs.Quad(it.Result()))
	}
	sort.Sort(res)
	return res
}

type ordered []quad.Quad

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
			o[i].Label < o[j].Label:

		return true

	default:
		return false
	}
}
func (o ordered) Swap(i, j int) { o[i], o[j] = o[j], o[i] }

func iteratedNames(qs graph.QuadStore, it graph.Iterator) []string {
	var res []string
	for graph.Next(it) {
		res = append(res, qs.NameOf(it.Result()))
	}
	sort.Strings(res)
	return res
}

func TestCreateDatabase(t *testing.T) {
	tmpFile, err := ioutil.TempFile(os.TempDir(), "cayley_test")
	if err != nil {
		t.Fatalf("Could not create working directory: %v", err)
	}
	t.Log(tmpFile)

	err = createNewBolt(tmpFile.Name(), nil)
	if err != nil {
		t.Fatal("Failed to create LevelDB database.")
	}

	qs, err := newQuadStore(tmpFile.Name(), nil)
	if qs == nil || err != nil {
		t.Error("Failed to create bolt QuadStore.")
	}
	if s := qs.Size(); s != 0 {
		t.Errorf("Unexpected size, got:%d expected:0", s)
	}
	qs.Close()

	err = createNewBolt("/dev/null/some terrible path", nil)
	if err == nil {
		t.Errorf("Created LevelDB database for bad path.")
	}

	os.RemoveAll(tmpFile.Name())
}

func TestLoadDatabase(t *testing.T) {
	tmpFile, err := ioutil.TempFile(os.TempDir(), "cayley_test")
	if err != nil {
		t.Fatalf("Could not create working directory: %v", err)
	}
	defer os.RemoveAll(tmpFile.Name())
	t.Log(tmpFile.Name())

	err = createNewBolt(tmpFile.Name(), nil)
	if err != nil {
		t.Fatal("Failed to create Bolt database.", err)
	}

	qs, err := newQuadStore(tmpFile.Name(), nil)
	if qs == nil || err != nil {
		t.Error("Failed to create Bolt QuadStore.")
	}

	w, _ := writer.NewSingleReplication(qs, nil)
	w.AddQuad(quad.Quad{
		Subject:   "Something",
		Predicate: "points_to",
		Object:    "Something Else",
		Label:     "context",
	})
	for _, pq := range []string{"Something", "points_to", "Something Else", "context"} {
		if got := qs.NameOf(qs.ValueOf(pq)); got != pq {
			t.Errorf("Failed to roundtrip %q, got:%q expect:%q", pq, got, pq)
		}
	}
	if s := qs.Size(); s != 1 {
		t.Errorf("Unexpected quadstore size, got:%d expect:1", s)
	}
	qs.Close()
	os.RemoveAll(tmpFile.Name())

	err = createNewBolt(tmpFile.Name(), nil)
	if err != nil {
		t.Fatal("Failed to create Bolt database.", err)
	}
	qs, err = newQuadStore(tmpFile.Name(), nil)
	if qs == nil || err != nil {
		t.Error("Failed to create Bolt QuadStore.")
	}
	w, _ = writer.NewSingleReplication(qs, nil)

	ts2, didConvert := qs.(*QuadStore)
	if !didConvert {
		t.Errorf("Could not convert from generic to Bolt QuadStore")
	}

	//Test horizon
	horizon := qs.Horizon()
	if horizon.Int() != 0 {
		t.Errorf("Unexpected horizon value, got:%d expect:0", horizon.Int())
	}

	w.AddQuadSet(makeQuadSet())
	if s := qs.Size(); s != 11 {
		t.Errorf("Unexpected quadstore size, got:%d expect:11", s)
	}
	if s := ts2.SizeOf(qs.ValueOf("B")); s != 5 {
		t.Errorf("Unexpected quadstore size, got:%d expect:5", s)
	}
	horizon = qs.Horizon()
	if horizon.Int() != 11 {
		t.Errorf("Unexpected horizon value, got:%d expect:11", horizon.Int())
	}

	w.RemoveQuad(quad.Quad{
		Subject:   "A",
		Predicate: "follows",
		Object:    "B",
		Label:     "",
	})
	if s := qs.Size(); s != 10 {
		t.Errorf("Unexpected quadstore size after RemoveQuad, got:%d expect:10", s)
	}
	if s := ts2.SizeOf(qs.ValueOf("B")); s != 4 {
		t.Errorf("Unexpected quadstore size, got:%d expect:4", s)
	}

	qs.Close()
}

func TestIterator(t *testing.T) {
	tmpFile, err := ioutil.TempFile(os.TempDir(), "cayley_test")
	if err != nil {
		t.Fatalf("Could not create working directory: %v", err)
	}
	defer os.RemoveAll(tmpFile.Name())
	t.Log(tmpFile.Name())

	err = createNewBolt(tmpFile.Name(), nil)
	if err != nil {
		t.Fatal("Failed to create LevelDB database.")
	}

	qs, err := newQuadStore(tmpFile.Name(), nil)
	if qs == nil || err != nil {
		t.Error("Failed to create bolt QuadStore.")
	}

	w, _ := writer.NewSingleReplication(qs, nil)
	w.AddQuadSet(makeQuadSet())
	var it graph.Iterator

	it = qs.NodesAllIterator()
	if it == nil {
		t.Fatal("Got nil iterator.")
	}

	size, _ := it.Size()
	if size <= 0 || size >= 20 {
		t.Errorf("Unexpected size, got:%d expect:(0, 20)", size)
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
		got := iteratedNames(qs, it)
		sort.Strings(got)
		if !reflect.DeepEqual(got, expect) {
			t.Errorf("Unexpected iterated result on repeat %d, got:%v expect:%v", i, got, expect)
		}
		it.Reset()
	}

	for _, pq := range expect {
		if !it.Contains(qs.ValueOf(pq)) {
			t.Errorf("Failed to find and check %q correctly", pq)
		}
	}
	// FIXME(kortschak) Why does this fail?
	/*
		for _, pq := range []string{"baller"} {
			if it.Contains(qs.ValueOf(pq)) {
				t.Errorf("Failed to check %q correctly", pq)
			}
		}
	*/
	it.Reset()

	it = qs.QuadsAllIterator()
	graph.Next(it)
	fmt.Printf("%#v\n", it.Result())
	q := qs.Quad(it.Result())
	fmt.Println(q)
	set := makeQuadSet()
	var ok bool
	for _, e := range set {
		if e.String() == q.String() {
			ok = true
			break
		}
	}
	if !ok {
		t.Errorf("Failed to find %q during iteration, got:%q", q, set)
	}

	qs.Close()
}

func TestSetIterator(t *testing.T) {

	tmpFile, _ := ioutil.TempFile(os.TempDir(), "cayley_test")
	t.Log(tmpFile.Name())
	defer os.RemoveAll(tmpFile.Name())
	err := createNewBolt(tmpFile.Name(), nil)
	if err != nil {
		t.Fatalf("Failed to create working directory")
	}

	qs, err := newQuadStore(tmpFile.Name(), nil)
	if qs == nil || err != nil {
		t.Error("Failed to create bolt QuadStore.")
	}
	defer qs.Close()

	w, _ := writer.NewSingleReplication(qs, nil)
	w.AddQuadSet(makeQuadSet())

	expect := []quad.Quad{
		{"C", "follows", "B", ""},
		{"C", "follows", "D", ""},
	}
	sort.Sort(ordered(expect))

	// Subject iterator.
	it := qs.QuadIterator(quad.Subject, qs.ValueOf("C"))

	if got := iteratedQuads(qs, it); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get expected results, got:%v expect:%v", got, expect)
	}
	it.Reset()

	and := iterator.NewAnd(qs)
	and.AddSubIterator(qs.QuadsAllIterator())
	and.AddSubIterator(it)

	if got := iteratedQuads(qs, and); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get confirm expected results, got:%v expect:%v", got, expect)
	}

	// Object iterator.
	it = qs.QuadIterator(quad.Object, qs.ValueOf("F"))

	expect = []quad.Quad{
		{"B", "follows", "F", ""},
		{"E", "follows", "F", ""},
	}
	sort.Sort(ordered(expect))
	if got := iteratedQuads(qs, it); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get expected results, got:%v expect:%v", got, expect)
	}

	and = iterator.NewAnd(qs)
	and.AddSubIterator(qs.QuadIterator(quad.Subject, qs.ValueOf("B")))
	and.AddSubIterator(it)

	expect = []quad.Quad{
		{"B", "follows", "F", ""},
	}
	if got := iteratedQuads(qs, and); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get confirm expected results, got:%v expect:%v", got, expect)
	}

	// Predicate iterator.
	it = qs.QuadIterator(quad.Predicate, qs.ValueOf("status"))

	expect = []quad.Quad{
		{"B", "status", "cool", "status_graph"},
		{"D", "status", "cool", "status_graph"},
		{"G", "status", "cool", "status_graph"},
	}
	sort.Sort(ordered(expect))
	if got := iteratedQuads(qs, it); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get expected results from predicate iterator, got:%v expect:%v", got, expect)
	}

	// Label iterator.
	it = qs.QuadIterator(quad.Label, qs.ValueOf("status_graph"))

	expect = []quad.Quad{
		{"B", "status", "cool", "status_graph"},
		{"D", "status", "cool", "status_graph"},
		{"G", "status", "cool", "status_graph"},
	}
	sort.Sort(ordered(expect))
	if got := iteratedQuads(qs, it); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get expected results from predicate iterator, got:%v expect:%v", got, expect)
	}
	it.Reset()

	// Order is important
	and = iterator.NewAnd(qs)
	and.AddSubIterator(qs.QuadIterator(quad.Subject, qs.ValueOf("B")))
	and.AddSubIterator(it)

	expect = []quad.Quad{
		{"B", "status", "cool", "status_graph"},
	}
	if got := iteratedQuads(qs, and); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get confirm expected results, got:%v expect:%v", got, expect)
	}
	it.Reset()

	// Order is important
	and = iterator.NewAnd(qs)
	and.AddSubIterator(it)
	and.AddSubIterator(qs.QuadIterator(quad.Subject, qs.ValueOf("B")))

	expect = []quad.Quad{
		{"B", "status", "cool", "status_graph"},
	}
	if got := iteratedQuads(qs, and); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get confirm expected results, got:%v expect:%v", got, expect)
	}
}

func TestOptimize(t *testing.T) {
	tmpFile, _ := ioutil.TempFile(os.TempDir(), "cayley_test")
	t.Log(tmpFile.Name())
	defer os.RemoveAll(tmpFile.Name())
	err := createNewBolt(tmpFile.Name(), nil)
	if err != nil {
		t.Fatalf("Failed to create working directory")
	}
	qs, err := newQuadStore(tmpFile.Name(), nil)
	if qs == nil || err != nil {
		t.Error("Failed to create bolt QuadStore.")
	}

	w, _ := writer.NewSingleReplication(qs, nil)
	w.AddQuadSet(makeQuadSet())

	// With an linksto-fixed pair
	fixed := qs.FixedIterator()
	fixed.Add(qs.ValueOf("F"))
	fixed.Tagger().Add("internal")
	lto := iterator.NewLinksTo(qs, fixed, quad.Object)

	oldIt := lto.Clone()
	newIt, ok := lto.Optimize()
	if !ok {
		t.Errorf("Failed to optimize iterator")
	}
	if newIt.Type() != Type() {
		t.Errorf("Optimized iterator type does not match original, got:%v expect:%v", newIt.Type(), Type())
	}

	newQuads := iteratedQuads(qs, newIt)
	oldQuads := iteratedQuads(qs, oldIt)
	if !reflect.DeepEqual(newQuads, oldQuads) {
		t.Errorf("Optimized iteration does not match original")
	}

	graph.Next(oldIt)
	oldResults := make(map[string]graph.Value)
	oldIt.TagResults(oldResults)
	graph.Next(newIt)
	newResults := make(map[string]graph.Value)
	newIt.TagResults(newResults)
	if !reflect.DeepEqual(newResults, oldResults) {
		t.Errorf("Discordant tag results, new:%v old:%v", newResults, oldResults)
	}
}

func TestDeletedFromIterator(t *testing.T) {

	tmpFile, _ := ioutil.TempFile(os.TempDir(), "cayley_test")
	t.Log(tmpFile.Name())
	defer os.RemoveAll(tmpFile.Name())
	err := createNewBolt(tmpFile.Name(), nil)
	if err != nil {
		t.Fatalf("Failed to create working directory")
	}

	qs, err := newQuadStore(tmpFile.Name(), nil)
	if qs == nil || err != nil {
		t.Error("Failed to create bolt QuadStore.")
	}
	defer qs.Close()

	w, _ := writer.NewSingleReplication(qs, nil)
	w.AddQuadSet(makeQuadSet())

	expect := []quad.Quad{
		{"E", "follows", "F", ""},
	}
	sort.Sort(ordered(expect))

	// Subject iterator.
	it := qs.QuadIterator(quad.Subject, qs.ValueOf("E"))

	if got := iteratedQuads(qs, it); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get expected results, got:%v expect:%v", got, expect)
	}
	it.Reset()

	w.RemoveQuad(quad.Quad{"E", "follows", "F", ""})
	expect = nil

	if got := iteratedQuads(qs, it); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get expected results, got:%v expect:%v", got, expect)
	}
}
