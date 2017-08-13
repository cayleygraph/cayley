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
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/path/pathtest"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/writer"
)

func TestCreateDatabase(t *testing.T) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "cayley_test")
	if err != nil {
		t.Fatalf("Could not create working directory: %v", err)
	}

	err = createNewLevelDB(tmpDir, nil)
	if err != nil {
		t.Fatal("Failed to create LevelDB database.")
	}

	qs, err := newQuadStore(tmpDir, nil)
	if qs == nil || err != nil {
		t.Error("Failed to create leveldb QuadStore.")
	}
	if s := qs.Size(); s != 0 {
		t.Errorf("Unexpected size, got:%d expected:0", s)
	}
	qs.Close()

	os.RemoveAll(tmpDir)
}

func makeLevelDB(t testing.TB) (graph.QuadStore, graph.Options, func()) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "cayley_test")
	if err != nil {
		t.Fatalf("Could not create working directory: %v", err)
	}
	err = createNewLevelDB(tmpDir, nil)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatal("Failed to create Bolt database.", err)
	}
	qs, err := newQuadStore(tmpDir, nil)
	if qs == nil || err != nil {
		os.RemoveAll(tmpDir)
		t.Fatal("Failed to create Bolt QuadStore.")
	}
	return qs, nil, func() {
		qs.Close()
		os.RemoveAll(tmpDir)
	}
}

func TestLevelDBAll(t *testing.T) {
	graphtest.TestAll(t, makeLevelDB, &graphtest.Config{
		NoPrimitives:            true,
		SkipDeletedFromIterator: true,
		SkipNodeDelAfterQuadDel: true,
	})
}

func TestLoadDatabase(t *testing.T) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "cayley_test")
	if err != nil {
		t.Fatalf("Could not create working directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	err = createNewLevelDB(tmpDir, nil)
	if err != nil {
		t.Fatal("Failed to create LevelDB database.")
	}

	qs, err := newQuadStore(tmpDir, nil)
	if qs == nil || err != nil {
		t.Error("Failed to create leveldb QuadStore.")
	}

	w, _ := writer.NewSingleReplication(qs, nil)
	err = w.AddQuad(quad.MakeRaw(
		"Something",
		"points_to",
		"Something Else",
		"context",
	))
	if err != nil {
		t.Fatal("Failed to add quad:", err)
	}
	for _, pq := range []string{"Something", "points_to", "Something Else", "context"} {
		if got := quad.StringOf(qs.NameOf(qs.ValueOf(quad.Raw(pq)))); got != pq {
			t.Errorf("Failed to roundtrip %q, got:%q expect:%q", pq, got, pq)
		}
	}
	if s := qs.Size(); s != 1 {
		t.Errorf("Unexpected quadstore size, got:%d expect:1", s)
	}
	qs.Close()

	err = createNewLevelDB(tmpDir, nil)
	if err != graph.ErrDatabaseExists {
		t.Fatal("Failed to create LevelDB database.")
	}
	qs, err = newQuadStore(tmpDir, nil)
	if qs == nil || err != nil {
		t.Error("Failed to create leveldb QuadStore.")
	}
	w, _ = writer.NewSingleReplication(qs, nil)

	ts2, didConvert := qs.(*QuadStore)
	if !didConvert {
		t.Errorf("Could not convert from generic to LevelDB QuadStore")
	}

	//Test horizon
	horizon := qs.Horizon()
	if v, _ := horizon.Int(); v != 1 {
		t.Errorf("Unexpected horizon value, got:%d expect:1", v)
	}

	w.AddQuadSet(graphtest.MakeQuadSet())
	if s := qs.Size(); s != 12 {
		t.Errorf("Unexpected quadstore size, got:%d expect:12", s)
	}
	if s := ts2.SizeOf(qs.ValueOf(quad.Raw("B"))); s != 5 {
		t.Errorf("Unexpected quadstore size, got:%d expect:5", s)
	}
	horizon = qs.Horizon()
	if v, _ := horizon.Int(); v != 12 {
		t.Errorf("Unexpected horizon value, got:%d expect:12", v)
	}

	w.RemoveQuad(quad.MakeRaw(
		"A",
		"follows",
		"B",
		"",
	))
	if s := qs.Size(); s != 11 {
		t.Errorf("Unexpected quadstore size after RemoveQuad, got:%d expect:11", s)
	}
	if s := ts2.SizeOf(qs.ValueOf(quad.Raw("B"))); s != 4 {
		t.Errorf("Unexpected quadstore size, got:%d expect:4", s)
	}

	qs.Close()
}

func TestOptimize(t *testing.T) {
	qs, opts, closer := makeLevelDB(t)
	defer closer()

	graphtest.MakeWriter(t, qs, opts, graphtest.MakeQuadSet()...)

	// With an linksto-fixed pair
	fixed := qs.FixedIterator()
	fixed.Add(qs.ValueOf(quad.Raw("F")))
	fixed.Tagger().Add("internal")
	lto := iterator.NewLinksTo(qs, fixed, quad.Object)

	oldIt := lto.Clone()
	newIt, ok := lto.Optimize()
	if !ok {
		t.Errorf("Failed to optimize iterator")
	}
	if _, ok := newIt.(*Iterator); !ok {
		t.Errorf("Optimized iterator type does not match original, got:%T", newIt)
	}

	newQuads := graphtest.IteratedQuads(t, qs, newIt)
	oldQuads := graphtest.IteratedQuads(t, qs, oldIt)
	if !reflect.DeepEqual(newQuads, oldQuads) {
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

func TestLevelDBPaths(t *testing.T) {
	pathtest.RunTestMorphisms(t, makeLevelDB)
}
