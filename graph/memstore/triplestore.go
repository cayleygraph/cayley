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
	"github.com/google/cayley/graph/memstore/b"
	"github.com/google/cayley/quad"
)

func init() {
	graph.RegisterTripleStore("memstore", false, func(string, graph.Options) (graph.TripleStore, error) {
		return newTripleStore(), nil
	}, nil)
}

type TripleDirectionIndex struct {
	index [4]map[int64]*b.Tree
}

func NewTripleDirectionIndex() TripleDirectionIndex {
	return TripleDirectionIndex{[...]map[int64]*b.Tree{
		quad.Subject - 1:   make(map[int64]*b.Tree),
		quad.Predicate - 1: make(map[int64]*b.Tree),
		quad.Object - 1:    make(map[int64]*b.Tree),
		quad.Label - 1:     make(map[int64]*b.Tree),
	}}
}

func (tdi TripleDirectionIndex) Tree(d quad.Direction, id int64) *b.Tree {
	if d < quad.Subject || d > quad.Label {
		panic("illegal direction")
	}
	tree, ok := tdi.index[d-1][id]
	if !ok {
		tree = b.TreeNew(cmp)
		tdi.index[d-1][id] = tree
	}
	return tree
}

func (tdi TripleDirectionIndex) Get(d quad.Direction, id int64) (*b.Tree, bool) {
	if d < quad.Subject || d > quad.Label {
		panic("illegal direction")
	}
	tree, ok := tdi.index[d-1][id]
	return tree, ok
}

type TripleStore struct {
	idCounter       int64
	tripleIdCounter int64
	idMap           map[string]int64
	revIdMap        map[int64]string
	triples         []quad.Quad
	size            int64
	index           TripleDirectionIndex
	// vip_index map[string]map[int64]map[string]map[int64]*b.Tree
}

func newTripleStore() *TripleStore {
	return &TripleStore{
		idMap:    make(map[string]int64),
		revIdMap: make(map[int64]string),

		// Sentinel null triple so triple indices start at 1
		triples: make([]quad.Quad, 1, 200),

		size:            1,
		index:           NewTripleDirectionIndex(),
		idCounter:       1,
		tripleIdCounter: 1,
	}
}

func (ts *TripleStore) AddTripleSet(triples []quad.Quad) {
	for _, t := range triples {
		ts.AddTriple(t)
	}
}

const maxInt = int(^uint(0) >> 1)

func (ts *TripleStore) indexOf(t quad.Quad) (int64, bool) {
	min := maxInt
	var tree *b.Tree
	for d := quad.Subject; d <= quad.Label; d++ {
		sid := t.Get(d)
		if d == quad.Label && sid == "" {
			continue
		}
		id, ok := ts.idMap[sid]
		// If we've never heard about a node, it must not exist
		if !ok {
			return 0, false
		}
		index, ok := ts.index.Get(d, id)
		if !ok {
			// If it's never been indexed in this direction, it can't exist.
			return 0, false
		}
		if l := index.Len(); l < min {
			min, tree = l, index
		}
	}
	it := NewIterator(tree, "")

	for it.Next() {
		val := it.Result()
		if t == ts.triples[val.(int64)] {
			return val.(int64), true
		}
	}
	return 0, false
}

func (ts *TripleStore) AddTriple(t quad.Quad) {
	if _, exists := ts.indexOf(t); exists {
		return
	}
	ts.triples = append(ts.triples, t)
	tid := ts.tripleIdCounter
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
		tree := ts.index.Tree(d, id)
		tree.Set(tid, struct{}{})
	}

	// TODO(barakmich): Add VIP indexing
}

func (ts *TripleStore) RemoveTriple(t quad.Quad) {
	tid, ok := ts.indexOf(t)
	if !ok {
		return
	}

	ts.triples[tid] = quad.Quad{}
	ts.size--

	for d := quad.Subject; d <= quad.Label; d++ {
		if d == quad.Label && t.Get(d) == "" {
			continue
		}
		id := ts.idMap[t.Get(d)]
		tree := ts.index.Tree(d, id)
		tree.Delete(tid)
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
			nodeTree := ts.index.Tree(d, id)
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

func (ts *TripleStore) Quad(index graph.Value) quad.Quad {
	return ts.triples[index.(int64)]
}

func (ts *TripleStore) TripleIterator(d quad.Direction, value graph.Value) graph.Iterator {
	index, ok := ts.index.Get(d, value.(int64))
	data := fmt.Sprintf("dir:%s val:%d", d, value.(int64))
	if ok {
		return NewIterator(index, data)
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
