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
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"

	"github.com/petar/GoLLRB/llrb"
)

func init() {
	graph.RegisterTripleStore("memstore", func(string, graph.Options) (graph.TripleStore, error) {
		return newTripleStore(), nil
	}, nil)
}

type TripleDirectionIndex struct {
	subject   map[int64]*llrb.LLRB
	predicate map[int64]*llrb.LLRB
	object    map[int64]*llrb.LLRB
	label     map[int64]*llrb.LLRB
}

func NewTripleDirectionIndex() *TripleDirectionIndex {
	var tdi TripleDirectionIndex
	tdi.subject = make(map[int64]*llrb.LLRB)
	tdi.predicate = make(map[int64]*llrb.LLRB)
	tdi.object = make(map[int64]*llrb.LLRB)
	tdi.label = make(map[int64]*llrb.LLRB)
	return &tdi
}

func (tdi *TripleDirectionIndex) GetForDir(d quad.Direction) map[int64]*llrb.LLRB {
	switch d {
	case quad.Subject:
		return tdi.subject
	case quad.Object:
		return tdi.object
	case quad.Predicate:
		return tdi.predicate
	case quad.Label:
		return tdi.label
	}
	panic("illegal direction")
}

func (tdi *TripleDirectionIndex) GetOrCreate(d quad.Direction, id int64) *llrb.LLRB {
	directionIndex := tdi.GetForDir(d)
	if _, ok := directionIndex[id]; !ok {
		directionIndex[id] = llrb.New()
	}
	return directionIndex[id]
}

func (tdi *TripleDirectionIndex) Get(d quad.Direction, id int64) (*llrb.LLRB, bool) {
	directionIndex := tdi.GetForDir(d)
	tree, exists := directionIndex[id]
	return tree, exists
}

type TripleStore struct {
	idCounter       int64
	tripleIdCounter int64
	idMap           map[string]int64
	revIdMap        map[int64]string
	triples         []quad.Quad
	size            int64
	index           TripleDirectionIndex
	// vip_index map[string]map[int64]map[string]map[int64]*llrb.Tree
}

func newTripleStore() *TripleStore {
	var ts TripleStore
	ts.idMap = make(map[string]int64)
	ts.revIdMap = make(map[int64]string)
	ts.triples = make([]quad.Quad, 1, 200)

	// Sentinel null triple so triple indices start at 1
	ts.triples[0] = quad.Quad{}
	ts.size = 1
	ts.index = *NewTripleDirectionIndex()
	ts.idCounter = 1
	ts.tripleIdCounter = 1
	return &ts
}

func (ts *TripleStore) AddTripleSet(triples []*quad.Quad) {
	for _, t := range triples {
		ts.AddTriple(t)
	}
}

func (ts *TripleStore) tripleExists(t *quad.Quad) (bool, int64) {
	smallest := -1
	var smallest_tree *llrb.LLRB
	for d := quad.Subject; d <= quad.Label; d++ {
		sid := t.Get(d)
		if d == quad.Label && sid == "" {
			continue
		}
		id, ok := ts.idMap[sid]
		// If we've never heard about a node, it most not exist
		if !ok {
			return false, 0
		}
		index, exists := ts.index.Get(d, id)
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

	for it.Next() {
		val := it.Result()
		if t.Equals(&ts.triples[val.(int64)]) {
			return true, val.(int64)
		}
	}
	return false, 0
}

func (ts *TripleStore) AddTriple(t *quad.Quad) {
	if exists, _ := ts.tripleExists(t); exists {
		return
	}
	var tripleID int64
	ts.triples = append(ts.triples, *t)
	tripleID = ts.tripleIdCounter
	ts.size++
	ts.tripleIdCounter++

	for d := quad.Subject; d <= quad.Label; d++ {
		sid := t.Get(d)
		if d == quad.Label && sid == "" {
			continue
		}
		if _, ok := ts.idMap[sid]; !ok {
			ts.idMap[sid] = ts.idCounter
			ts.revIdMap[ts.idCounter] = sid
			ts.idCounter++
		}
	}

	for d := quad.Subject; d <= quad.Label; d++ {
		if d == quad.Label && t.Get(d) == "" {
			continue
		}
		id := ts.idMap[t.Get(d)]
		tree := ts.index.GetOrCreate(d, id)
		tree.ReplaceOrInsert(Int64(tripleID))
	}

	// TODO(barakmich): Add VIP indexing
}

func (ts *TripleStore) RemoveTriple(t *quad.Quad) {
	var tripleID int64
	var exists bool
	tripleID = 0
	if exists, tripleID = ts.tripleExists(t); !exists {
		return
	}

	ts.triples[tripleID] = quad.Quad{}
	ts.size--

	for d := quad.Subject; d <= quad.Label; d++ {
		if d == quad.Label && t.Get(d) == "" {
			continue
		}
		id := ts.idMap[t.Get(d)]
		tree := ts.index.GetOrCreate(d, id)
		tree.Delete(Int64(tripleID))
	}

	for d := quad.Subject; d <= quad.Label; d++ {
		if d == quad.Label && t.Get(d) == "" {
			continue
		}
		id, ok := ts.idMap[t.Get(d)]
		if !ok {
			continue
		}
		stillExists := false
		for d := quad.Subject; d <= quad.Label; d++ {
			if d == quad.Label && t.Get(d) == "" {
				continue
			}
			nodeTree := ts.index.GetOrCreate(d, id)
			if nodeTree.Len() != 0 {
				stillExists = true
				break
			}
		}
		if !stillExists {
			delete(ts.idMap, t.Get(d))
			delete(ts.revIdMap, id)
		}
	}
}

func (ts *TripleStore) Quad(index graph.Value) *quad.Quad {
	return &ts.triples[index.(int64)]
}

func (ts *TripleStore) TripleIterator(d quad.Direction, value graph.Value) graph.Iterator {
	index, ok := ts.index.Get(d, value.(int64))
	data := fmt.Sprintf("dir:%s val:%d", d, value.(int64))
	if ok {
		return NewLlrbIterator(index, data)
	}
	return &iterator.Null{}
}

func (ts *TripleStore) Size() int64 {
	return ts.size - 1 // Don't count the sentinel
}

func (ts *TripleStore) DebugPrint() {
	for i, t := range ts.triples {
		if i == 0 {
			continue
		}
		glog.V(2).Infof("%d: %s", i, t)
	}
}

func (ts *TripleStore) ValueOf(name string) graph.Value {
	return ts.idMap[name]
}

func (ts *TripleStore) NameOf(id graph.Value) string {
	return ts.revIdMap[id.(int64)]
}

func (ts *TripleStore) TriplesAllIterator() graph.Iterator {
	return iterator.NewInt64(0, ts.Size())
}

func (ts *TripleStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixedIteratorWithCompare(iterator.BasicEquality)
}

func (ts *TripleStore) TripleDirection(val graph.Value, d quad.Direction) graph.Value {
	name := ts.Quad(val).Get(d)
	return ts.ValueOf(name)
}

func (ts *TripleStore) NodesAllIterator() graph.Iterator {
	return NewMemstoreAllIterator(ts)
}
func (ts *TripleStore) Close() {}
