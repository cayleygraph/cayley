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

type QuadDirectionIndex struct {
	index [4]map[int64]*b.Tree
}

func NewQuadDirectionIndex() QuadDirectionIndex {
	return QuadDirectionIndex{[...]map[int64]*b.Tree{
		quad.Subject - 1:   make(map[int64]*b.Tree),
		quad.Predicate - 1: make(map[int64]*b.Tree),
		quad.Object - 1:    make(map[int64]*b.Tree),
		quad.Label - 1:     make(map[int64]*b.Tree),
	}}
}

func (qdi QuadDirectionIndex) Tree(d quad.Direction, id int64) *b.Tree {
	if d < quad.Subject || d > quad.Label {
		panic("illegal direction")
	}
	tree, ok := qdi.index[d-1][id]
	if !ok {
		tree = b.TreeNew(cmp)
		qdi.index[d-1][id] = tree
	}
	return tree
}

func (qdi QuadDirectionIndex) Get(d quad.Direction, id int64) (*b.Tree, bool) {
	if d < quad.Subject || d > quad.Label {
		panic("illegal direction")
	}
	tree, ok := qdi.index[d-1][id]
	return tree, ok
}

type LogEntry struct {
	graph.Delta
	DeletedBy int64
}

type TripleStore struct {
	idCounter     int64
	quadIdCounter int64
	idMap         map[string]int64
	revIdMap      map[int64]string
	log           []LogEntry
	size          int64
	index         QuadDirectionIndex
	// vip_index map[string]map[int64]map[string]map[int64]*b.Tree
}

func newTripleStore() *TripleStore {
	return &TripleStore{
		idMap:    make(map[string]int64),
		revIdMap: make(map[int64]string),

		// Sentinel null entry so indices start at 1
		log: make([]LogEntry, 1, 200),

		index:         NewQuadDirectionIndex(),
		idCounter:     1,
		quadIdCounter: 1,
	}
}

func (ts *TripleStore) ApplyDeltas(deltas []graph.Delta) error {
	for _, d := range deltas {
		var err error
		if d.Action == graph.Add {
			err = ts.AddDelta(d)
		} else {
			err = ts.RemoveDelta(d)
		}
		if err != nil {
			return err
		}
	}
	return nil
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
	it := NewIterator(tree, "", ts)

	for it.Next() {
		val := it.Result()
		if t == ts.log[val.(int64)].Quad {
			return val.(int64), true
		}
	}
	return 0, false
}

func (ts *TripleStore) AddDelta(d graph.Delta) error {
	if _, exists := ts.indexOf(d.Quad); exists {
		return graph.ErrQuadExists
	}
	qid := ts.quadIdCounter
	ts.log = append(ts.log, LogEntry{Delta: d})
	ts.size++
	ts.quadIdCounter++

	for dir := quad.Subject; dir <= quad.Label; dir++ {
		sid := d.Quad.Get(dir)
		if dir == quad.Label && sid == "" {
			continue
		}
		if _, ok := ts.idMap[sid]; !ok {
			ts.idMap[sid] = ts.idCounter
			ts.revIdMap[ts.idCounter] = sid
			ts.idCounter++
		}
	}

	for dir := quad.Subject; dir <= quad.Label; dir++ {
		if dir == quad.Label && d.Quad.Get(dir) == "" {
			continue
		}
		id := ts.idMap[d.Quad.Get(dir)]
		tree := ts.index.Tree(dir, id)
		tree.Set(qid, struct{}{})
	}

	// TODO(barakmich): Add VIP indexing
	return nil
}

func (ts *TripleStore) RemoveDelta(d graph.Delta) error {
	prevQuadID, exists := ts.indexOf(d.Quad)
	if !exists {
		return graph.ErrQuadNotExist
	}

	quadID := ts.quadIdCounter
	ts.log = append(ts.log, LogEntry{Delta: d})
	ts.log[prevQuadID].DeletedBy = quadID
	ts.size--
	ts.quadIdCounter++
	return nil
}

func (ts *TripleStore) Quad(index graph.Value) quad.Quad {
	return ts.log[index.(int64)].Quad
}

func (ts *TripleStore) TripleIterator(d quad.Direction, value graph.Value) graph.Iterator {
	index, ok := ts.index.Get(d, value.(int64))
	data := fmt.Sprintf("dir:%s val:%d", d, value.(int64))
	if ok {
		return NewIterator(index, data, ts)
	}
	return &iterator.Null{}
}

func (ts *TripleStore) Horizon() int64 {
	return ts.log[len(ts.log)-1].ID
}

func (ts *TripleStore) Size() int64 {
	return ts.size
}

func (ts *TripleStore) DebugPrint() {
	for i, l := range ts.log {
		if i == 0 {
			continue
		}
		glog.V(2).Infof("%d: %#v", i, l)
	}
}

func (ts *TripleStore) ValueOf(name string) graph.Value {
	return ts.idMap[name]
}

func (ts *TripleStore) NameOf(id graph.Value) string {
	return ts.revIdMap[id.(int64)]
}

func (ts *TripleStore) TriplesAllIterator() graph.Iterator {
	return NewMemstoreQuadsAllIterator(ts)
}

func (ts *TripleStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixedIteratorWithCompare(iterator.BasicEquality)
}

func (ts *TripleStore) TripleDirection(val graph.Value, d quad.Direction) graph.Value {
	name := ts.Quad(val).Get(d)
	return ts.ValueOf(name)
}

func (ts *TripleStore) NodesAllIterator() graph.Iterator {
	return NewMemstoreNodesAllIterator(ts)
}

func (ts *TripleStore) Close() {}
