// Copyright 2014 The Cayley Authors. All rights reserved.  //
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
// +build appengine

package gaedatastore

import (
	"errors"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/barakmich/glog"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
	"github.com/google/cayley/writer"

	"google.golang.org/appengine/aetest"
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
var simpleGraph = []quad.Quad{
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
var simpleGraphUpdate = []quad.Quad{
	{"A", "follows", "B", ""},
	{"F", "follows", "B", ""},
	{"C", "follows", "D", ""},
	{"X", "follows", "B", ""},
}

type pair struct {
	query string
	value int64
}

func makeTestStore(data []quad.Quad, opts graph.Options) (graph.QuadStore, graph.QuadWriter, []pair) {
	seen := make(map[string]struct{})

	qs, _ := newQuadStore("", opts)
	qs, _ = newQuadStoreForRequest(qs, opts)
	var (
		val int64
		ind []pair
	)
	writer, _ := writer.NewSingleReplication(qs, nil)
	for _, t := range data {
		for _, qp := range []string{t.Subject, t.Predicate, t.Object, t.Label} {
			if _, ok := seen[qp]; !ok && qp != "" {
				val++
				ind = append(ind, pair{qp, val})
				seen[qp] = struct{}{}
			}
		}
	}
	writer.AddQuadSet(data)
	return qs, writer, ind
}

func iterateResults(qs graph.QuadStore, it graph.Iterator) []string {
	var res []string
	for graph.Next(it) {
		v := it.Result()
		if t, ok := v.(*Token); ok && t.Kind == nodeKind {
			res = append(res, qs.NameOf(it.Result()))
		} else {
			res = append(res, qs.Quad(it.Result()).String())
		}
	}
	sort.Strings(res)
	it.Reset()
	return res
}

func printIterator(qs graph.QuadStore, it graph.Iterator) {
	for graph.Next(it) {
		glog.Infof("%v", qs.Quad(it.Result()))
	}
}

func compareResults(qs graph.QuadStore, it graph.Iterator, expect []string) ([]string, bool) {
	sort.Strings(expect)
	for i := 0; i < 2; i++ {
		got := iterateResults(qs, it)
		sort.Strings(got)
		if !reflect.DeepEqual(got, expect) {
			return got, false
		}
	}
	return nil, true
}

func createInstance() (aetest.Instance, graph.Options, error) {
	inst, err := aetest.NewInstance(&aetest.Options{"", true})
	if err != nil {
		return nil, nil, errors.New("Creation of new instance failed")
	}
	req1, err := inst.NewRequest("POST", "/api/v1/write", nil)
	if err != nil {
		return nil, nil, errors.New("Creation of new request failed")
	}
	opts := make(graph.Options)
	opts["HTTPRequest"] = req1
	return inst, opts, nil
}

func TestTransaction(t *testing.T) {
	glog.Info("\n-----------\n")
	inst, opts, err := createInstance()
	if err != nil {
		t.Fatalf("failed to create instance: %v", err)
	}
	defer inst.Close()

	qs, w, _ := makeTestStore(simpleGraph, opts)
	size := qs.Size()

	tx := graph.NewTransaction()
	tx.AddQuad(quad.Quad{
		Subject:   "E",
		Predicate: "follows",
		Object:    "G",
		Label:     ""})
	tx.RemoveQuad(quad.Quad{
		Subject:   "Non",
		Predicate: "existent",
		Object:    "quad",
		Label:     ""})

	err = w.ApplyTransaction(tx)
	if err == nil {
		t.Error("Able to remove a non-existent quad")
	}
	if size != qs.Size() {
		t.Error("Appended a new quad in a failed transaction")
	}
}

func TestAddRemove(t *testing.T) {
	inst, opts, err := createInstance()
	if err != nil {
		t.Fatalf("failed to create instance: %v", err)
	}
	defer inst.Close()

	// Add quads
	qs, writer, _ := makeTestStore(simpleGraph, opts)
	if qs.Size() != 11 {
		t.Fatalf("Incorrect number of quads: want %v have %v", 11, qs.Size())
	}
	all := qs.NodesAllIterator()
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
	if got, ok := compareResults(qs, all, expect); !ok {
		t.Errorf("Unexpected iterated result, got:%v expect:%v", got, expect)
	}

	// Add more quads, some conflicts
	if err := writer.AddQuadSet(simpleGraphUpdate); err != nil {
		t.Errorf("AddQuadSet failed, %v", err)
	}
	if qs.Size() != 13 {
		t.Fatalf("Incorrect number of quads, want %v got %v", 13, qs.Size())
	}
	all = qs.NodesAllIterator()
	expect = []string{
		"A",
		"B",
		"C",
		"D",
		"E",
		"F",
		"G",
		"X",
		"follows",
		"status",
		"cool",
		"status_graph",
	}
	if got, ok := compareResults(qs, all, expect); !ok {
		t.Errorf("Unexpected iterated result, got:%v expect:%v", got, expect)
	}

	// Remove quad
	toRemove := quad.Quad{"X", "follows", "B", ""}
	err = writer.RemoveQuad(toRemove)
	if err != nil {
		t.Errorf("RemoveQuad failed: %v", err)
	}
	expect = []string{
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
	if got, ok := compareResults(qs, all, expect); !ok {
		t.Errorf("Unexpected iterated result, got:%v expect:%v", got, expect)
	}

	addQuad := quad.Quad{"X", "follows", "B", ""}
	err = writer.AddQuad(addQuad)
	if err != nil {
		t.Errorf("AddQuad failed: %v", err)
	}
	expect = []string{
		"A",
		"B",
		"C",
		"D",
		"E",
		"F",
		"G",
		"X",
		"follows",
		"status",
		"cool",
		"status_graph",
	}
	if got, ok := compareResults(qs, all, expect); !ok {
		t.Errorf("Unexpected iterated result, got:%v expect:%v", got, expect)
	}
	set := []quad.Quad{}
	for i := 0; i < 100; i++ {
		set = append(set, quad.Quad{"X", "follows", "B" + strconv.Itoa(i), ""})
	}
	if err := writer.AddQuadSet(set); err != nil {
		t.Errorf("AddQuadSet failed, %v", err)
	}
	if qs.Size() != 113 {
		t.Fatalf("Incorrect number of quads, want %v got %v", 113, qs.Size())
	}
}

func TestIterators(t *testing.T) {
	glog.Info("\n-----------\n")
	inst, opts, err := createInstance()
	if err != nil {
		t.Fatalf("failed to create instance: %v", err)
	}
	defer inst.Close()

	qs, _, _ := makeTestStore(simpleGraph, opts)
	if qs.Size() != 11 {
		t.Fatal("Incorrect number of quads")
	}

	var expected = []string{
		quad.Quad{"C", "follows", "B", ""}.String(),
		quad.Quad{"C", "follows", "D", ""}.String(),
	}

	it := qs.QuadIterator(quad.Subject, qs.ValueOf("C"))
	if got, ok := compareResults(qs, it, expected); !ok {
		t.Errorf("Unexpected iterated result, got:%v expect:%v", got, expected)
	}

	// Test contains
	it = qs.QuadIterator(quad.Label, qs.ValueOf("status_graph"))
	gqs := qs.(*QuadStore)
	key := gqs.createKeyForQuad(quad.Quad{"G", "status", "cool", "status_graph"})
	token := &Token{quadKind, key.StringID()}
	if !it.Contains(token) {
		t.Error("Contains failed")
	}

	// Test cloning an iterator
	var it2 graph.Iterator
	it2 = it.Clone()
	x := it2.Describe()
	y := it.Describe()

	if x.Name != y.Name {
		t.Errorf("Iterator Clone was not successful got: %v, expected: %v", x.Name, y.Name)
	}
}

func TestIteratorsAndNextResultOrderA(t *testing.T) {
	glog.Info("\n-----------\n")
	inst, opts, err := createInstance()
	if err != nil {
		t.Fatalf("failed to create instance: %v", err)
	}
	defer inst.Close()

	qs, _, _ := makeTestStore(simpleGraph, opts)
	if qs.Size() != 11 {
		t.Fatal("Incorrect number of quads")
	}

	fixed := qs.FixedIterator()
	fixed.Add(qs.ValueOf("C"))

	fixed2 := qs.FixedIterator()
	fixed2.Add(qs.ValueOf("follows"))

	all := qs.NodesAllIterator()

	innerAnd := iterator.NewAnd(qs)
	innerAnd.AddSubIterator(iterator.NewLinksTo(qs, fixed2, quad.Predicate))
	innerAnd.AddSubIterator(iterator.NewLinksTo(qs, all, quad.Object))

	hasa := iterator.NewHasA(qs, innerAnd, quad.Subject)
	outerAnd := iterator.NewAnd(qs)
	outerAnd.AddSubIterator(fixed)
	outerAnd.AddSubIterator(hasa)

	if !outerAnd.Next() {
		t.Error("Expected one matching subtree")
	}
	val := outerAnd.Result()
	if qs.NameOf(val) != "C" {
		t.Errorf("Matching subtree should be %s, got %s", "barak", qs.NameOf(val))
	}

	var (
		got    []string
		expect = []string{"B", "D"}
	)
	for {
		got = append(got, qs.NameOf(all.Result()))
		if !outerAnd.NextPath() {
			break
		}
	}
	sort.Strings(got)

	if !reflect.DeepEqual(got, expect) {
		t.Errorf("Unexpected result, got:%q expect:%q", got, expect)
	}

	if outerAnd.Next() {
		t.Error("More than one possible top level output?")
	}

}
