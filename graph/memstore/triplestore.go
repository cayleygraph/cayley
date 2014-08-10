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
	graph.RegisterTripleStore("memstore", false, func(string, graph.Options) (graph.TripleStore, error) {
		return newTripleStore(), nil
	}, nil)
}

type QuadDirectionIndex struct {
	subject   map[int64]*llrb.LLRB
	predicate map[int64]*llrb.LLRB
	object    map[int64]*llrb.LLRB
	label     map[int64]*llrb.LLRB
}

func NewQuadDirectionIndex() *QuadDirectionIndex {
	var qdi QuadDirectionIndex
	qdi.subject = make(map[int64]*llrb.LLRB)
	qdi.predicate = make(map[int64]*llrb.LLRB)
	qdi.object = make(map[int64]*llrb.LLRB)
	qdi.label = make(map[int64]*llrb.LLRB)
	return &qdi
}

func (qdi *QuadDirectionIndex) GetForDir(d quad.Direction) map[int64]*llrb.LLRB {
	switch d {
	case quad.Subject:
		return qdi.subject
	case quad.Object:
		return qdi.object
	case quad.Predicate:
		return qdi.predicate
	case quad.Label:
		return qdi.label
	}
	panic("illegal direction")
}

func (qdi *QuadDirectionIndex) GetOrCreate(d quad.Direction, id int64) *llrb.LLRB {
	directionIndex := qdi.GetForDir(d)
	if _, ok := directionIndex[id]; !ok {
		directionIndex[id] = llrb.New()
	}
	return directionIndex[id]
}

func (qdi *QuadDirectionIndex) Get(d quad.Direction, id int64) (*llrb.LLRB, bool) {
	directionIndex := qdi.GetForDir(d)
	tree, exists := directionIndex[id]
	return tree, exists
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
	// vip_index map[string]map[int64]map[string]map[int64]*llrb.Tree
}

func newTripleStore() *TripleStore {
	var ts TripleStore
	ts.idMap = make(map[string]int64)
	ts.revIdMap = make(map[int64]string)
	ts.log = make([]LogEntry, 1, 200)

	// Sentinel null entry so indices start at 1
	ts.log[0] = LogEntry{}
	ts.index = *NewQuadDirectionIndex()
	ts.idCounter = 1
	ts.quadIdCounter = 1
	return &ts
}

func (ts *TripleStore) ApplyDeltas(deltas []*graph.Delta) error {
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

func (ts *TripleStore) quadExists(t quad.Quad) (bool, int64) {
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
	it := NewLlrbIterator(smallest_tree, "", ts)

	for it.Next() {
		val := it.Result()
		if t == ts.log[val.(int64)].Quad {
			return true, val.(int64)
		}
	}
	return false, 0
}

func (ts *TripleStore) AddDelta(d *graph.Delta) error {
	if exists, _ := ts.quadExists(d.Quad); exists {
		return graph.ErrQuadExists
	}
	var quadID int64
	quadID = ts.quadIdCounter
	ts.log = append(ts.log, LogEntry{Delta: *d})
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
		tree := ts.index.GetOrCreate(dir, id)
		tree.ReplaceOrInsert(Int64(quadID))
	}

	// TODO(barakmich): Add VIP indexing
	return nil
}

func (ts *TripleStore) RemoveDelta(d *graph.Delta) error {
	var prevQuadID int64
	var exists bool
	prevQuadID = 0
	if exists, prevQuadID = ts.quadExists(d.Quad); !exists {
		return graph.ErrQuadNotExist
	}

	var quadID int64
	quadID = ts.quadIdCounter
	ts.log = append(ts.log, LogEntry{Delta: *d})
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
		return NewLlrbIterator(index, data, ts)
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
