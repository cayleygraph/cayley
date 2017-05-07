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

package bolt2

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/kv"
	"github.com/cayleygraph/cayley/graph/path/pathtest"
	"github.com/cayleygraph/cayley/quad"
)

//var _ graphtest.ValueSizer = (*QuadStore)(nil)

func TestCreateDatabase(t *testing.T) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "cayley_test_bolt2")
	if err != nil {
		t.Fatalf("Could not create working directory: %v", err)
	}
	t.Log(tmpDir)

	err = createNewBolt(tmpDir, nil)
	if err != nil {
		t.Fatal("Failed to create Bolt database.")
	}

	qs, err := newQuadStore(tmpDir, nil)
	if qs == nil || err != nil {
		t.Error("Failed to create bolt QuadStore.")
	}
	if s := qs.Size(); s != 0 {
		t.Errorf("Unexpected size, got:%d expected:0", s)
	}
	qs.Close()

	os.RemoveAll(tmpDir)
}

func makeBolt(t testing.TB) (graph.QuadStore, graph.Options, func()) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "cayley_test_bolt2")
	if err != nil {
		t.Fatalf("Could not create working directory: %v", err)
	}
	err = createNewBolt(tmpDir, nil)
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

func TestBoltAll(t *testing.T) {
	graphtest.TestAll(t, makeBolt, &graphtest.Config{
		SkipNodeDelAfterQuadDel: true,
		SkipIntHorizon:          true,
	})
}

func TestOptimize(t *testing.T) {
	qs, opts, closer := makeBolt(t)
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
	if newIt.Type() != kv.Type() {
		t.Errorf("Optimized iterator type does not match original, got:%v expect:%v", newIt.Type(), kv.Type())
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

func TestBoltPaths(t *testing.T) {
	pathtest.RunTestMorphisms(t, makeBolt)
}
