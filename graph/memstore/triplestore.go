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
	"fmt"

	"github.com/barakmich/glog"
	"github.com/google/cayley/graph"

	"github.com/petar/GoLLRB/llrb"
)

type TripleDirectionIndex struct {
	subject    map[int64]*llrb.LLRB
	predicate  map[int64]*llrb.LLRB
	object     map[int64]*llrb.LLRB
	provenance map[int64]*llrb.LLRB
}

func NewTripleDirectionIndex() *TripleDirectionIndex {
	var tdi TripleDirectionIndex
	tdi.subject = make(map[int64]*llrb.LLRB)
	tdi.predicate = make(map[int64]*llrb.LLRB)
	tdi.object = make(map[int64]*llrb.LLRB)
	tdi.provenance = make(map[int64]*llrb.LLRB)
	return &tdi
}

func (tdi *TripleDirectionIndex) GetForDir(s string) map[int64]*llrb.LLRB {
	if s == "s" {
		return tdi.subject
	} else if s == "o" {
		return tdi.object
	} else if s == "p" {
		return tdi.predicate
	} else if s == "c" {
		return tdi.provenance
	}
	panic("Bad direction")
}

func (tdi *TripleDirectionIndex) GetOrCreate(dir string, id int64) *llrb.LLRB {
	directionIndex := tdi.GetForDir(dir)
	if _, ok := directionIndex[id]; !ok {
		directionIndex[id] = llrb.New()
	}
	return directionIndex[id]
}

func (tdi *TripleDirectionIndex) Get(dir string, id int64) (*llrb.LLRB, bool) {
	directionIndex := tdi.GetForDir(dir)
	tree, exists := directionIndex[id]
	return tree, exists
}

type TripleStore struct {
	idCounter       int64
	tripleIdCounter int64
	idMap           map[string]int64
	revIdMap        map[int64]string
	triples         []graph.Triple
	size            int64
	index           TripleDirectionIndex
	// vip_index map[string]map[int64]map[string]map[int64]*llrb.Tree
}

func NewTripleStore() *TripleStore {
	var ts TripleStore
	ts.idMap = make(map[string]int64)
	ts.revIdMap = make(map[int64]string)
	ts.triples = make([]graph.Triple, 1, 200)

	// Sentinel null triple so triple indices start at 1
	ts.triples[0] = graph.Triple{}
	ts.size = 1
	ts.index = *NewTripleDirectionIndex()
	ts.idCounter = 1
	ts.tripleIdCounter = 1
	return &ts
}

func (ts *TripleStore) AddTripleSet(triples []*graph.Triple) {
	for _, t := range triples {
		ts.AddTriple(t)
	}
}

func (ts *TripleStore) tripleExists(t *graph.Triple) (bool, int64) {
	smallest := -1
	var smallest_tree *llrb.LLRB
	for _, dir := range graph.TripleDirections {
		sid := t.Get(dir)
		if dir == "c" && sid == "" {
			continue
		}
		id, ok := ts.idMap[sid]
		// If we've never heard about a node, it most not exist
		if !ok {
			return false, 0
		}
		index, exists := ts.index.Get(dir, id)
		if !exists {
			// If it's never been indexed in this direction, it can't exist.
			return false, 0
		}
		if smallest == -1 || index.Len() < smallest {
			smallest = index.Len()
			smallest_tree = index
		}
	}
	it := NewLlrbIterator(smallest_tree, "")

	for {
		val, ok := it.Next()
		if !ok {
			break
		}
		if t.Equals(&ts.triples[val.(int64)]) {
			return true, val.(int64)
		}
	}
	return false, 0
}

func (ts *TripleStore) AddTriple(t *graph.Triple) {
	if exists, _ := ts.tripleExists(t); exists {
		return
	}
	var tripleID int64
	ts.triples = append(ts.triples, *t)
	tripleID = ts.tripleIdCounter
	ts.size++
	ts.tripleIdCounter++

	for _, dir := range graph.TripleDirections {
		sid := t.Get(dir)
		if dir == "c" && sid == "" {
			continue
		}
		if _, ok := ts.idMap[sid]; !ok {
			ts.idMap[sid] = ts.idCounter
			ts.revIdMap[ts.idCounter] = sid
			ts.idCounter++
		}
	}

	for _, dir := range graph.TripleDirections {
		if dir == "c" && t.Get(dir) == "" {
			continue
		}
		id := ts.idMap[t.Get(dir)]
		tree := ts.index.GetOrCreate(dir, id)
		tree.ReplaceOrInsert(Int64(tripleID))
	}

	// TODO(barakmich): Add VIP indexing
}

func (ts *TripleStore) RemoveTriple(t *graph.Triple) {
	var tripleID int64
	var exists bool
	tripleID = 0
	if exists, tripleID = ts.tripleExists(t); !exists {
		return
	}

	ts.triples[tripleID] = graph.Triple{}
	ts.size--

	for _, dir := range graph.TripleDirections {
		if dir == "c" && t.Get(dir) == "" {
			continue
		}
		id := ts.idMap[t.Get(dir)]
		tree := ts.index.GetOrCreate(dir, id)
		tree.Delete(Int64(tripleID))
	}

	for _, dir := range graph.TripleDirections {
		if dir == "c" && t.Get(dir) == "" {
			continue
		}
		id, ok := ts.idMap[t.Get(dir)]
		if !ok {
			continue
		}
		stillExists := false
		for _, dir := range graph.TripleDirections {
			if dir == "c" && t.Get(dir) == "" {
				continue
			}
			nodeTree := ts.index.GetOrCreate(dir, id)
			if nodeTree.Len() != 0 {
				stillExists = true
				break
			}
		}
		if !stillExists {
			delete(ts.idMap, t.Get(dir))
			delete(ts.revIdMap, id)
		}
	}
}

func (ts *TripleStore) GetTriple(index graph.TSVal) *graph.Triple {
	return &ts.triples[index.(int64)]
}

func (ts *TripleStore) GetTripleIterator(direction string, value graph.TSVal) graph.Iterator {
	index, ok := ts.index.Get(direction, value.(int64))
	data := fmt.Sprintf("dir:%s val:%d", direction, value.(int64))
	if ok {
		return NewLlrbIterator(index, data)
	}
	return &graph.NullIterator{}
}

func (ts *TripleStore) Size() int64 {
	return ts.size - 1 // Don't count the sentinel
}

func (ts *TripleStore) DebugPrint() {
	for i, t := range ts.triples {
		if i == 0 {
			continue
		}
		glog.V(2).Infoln("%d: %s", i, t.ToString())
	}
}

func (ts *TripleStore) GetIdFor(name string) graph.TSVal {
	return ts.idMap[name]
}

func (ts *TripleStore) GetNameFor(id graph.TSVal) string {
	return ts.revIdMap[id.(int64)]
}

func (ts *TripleStore) GetTriplesAllIterator() graph.Iterator {
	return graph.NewInt64AllIterator(0, ts.Size())
}

func (ts *TripleStore) MakeFixed() *graph.FixedIterator {
	return graph.NewFixedIteratorWithCompare(graph.BasicEquality)
}

func (ts *TripleStore) GetTripleDirection(val graph.TSVal, direction string) graph.TSVal {
	name := ts.GetTriple(val).Get(direction)
	return ts.GetIdFor(name)
}

func (ts *TripleStore) GetNodesAllIterator() graph.Iterator {
	return NewMemstoreAllIterator(ts)
}
func (ts *TripleStore) Close() {}
