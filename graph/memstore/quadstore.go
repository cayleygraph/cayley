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
	"errors"
	"fmt"
	"time"

	"github.com/barakmich/glog"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/graph/memstore/b"
	"github.com/google/cayley/quad"
)

const QuadStoreType = "memstore"

func init() {
	graph.RegisterQuadStore(QuadStoreType, graph.QuadStoreRegistration{
		NewFunc: func(string, graph.Options) (graph.QuadStore, error) {
			return newQuadStore(), nil
		},
		NewForRequestFunc: nil,
		UpgradeFunc:       nil,
		InitFunc:          nil,
		IsPersistent:      false,
	})
}

func cmp(a, b int64) int {
	return int(a - b)
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
	ID        int64
	Quad      quad.Quad
	Action    graph.Procedure
	Timestamp time.Time
	DeletedBy int64
}

type QuadStore struct {
	nextID     int64
	nextQuadID int64
	idMap      map[string]int64
	revIDMap   map[int64]string
	log        []LogEntry
	size       int64
	index      QuadDirectionIndex
	// vip_index map[string]map[int64]map[string]map[int64]*b.Tree
}

func newQuadStore() *QuadStore {
	return &QuadStore{
		idMap:    make(map[string]int64),
		revIDMap: make(map[int64]string),

		// Sentinel null entry so indices start at 1
		log: make([]LogEntry, 1, 200),

		index:      NewQuadDirectionIndex(),
		nextID:     1,
		nextQuadID: 1,
	}
}

func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta, ignoreOpts graph.IgnoreOpts) error {
	// Precheck the whole transaction
	for _, d := range deltas {
		switch d.Action {
		case graph.Add:
			if !ignoreOpts.IgnoreDup {
				if _, exists := qs.indexOf(d.Quad); exists {
					return graph.ErrQuadExists
				}
			}
		case graph.Delete:
			if !ignoreOpts.IgnoreMissing {
				if _, exists := qs.indexOf(d.Quad); !exists {
					return graph.ErrQuadNotExist
				}
			}
		default:
			return errors.New("memstore: invalid action")
		}
	}

	for _, d := range deltas {
		var err error
		switch d.Action {
		case graph.Add:
			err = qs.AddDelta(d)
			if err != nil && ignoreOpts.IgnoreDup {
				err = nil
			}
		case graph.Delete:
			err = qs.RemoveDelta(d)
			if err != nil && ignoreOpts.IgnoreMissing {
				err = nil
			}
		default:
			panic("memstore: unexpected invalid action")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

const maxInt = int(^uint(0) >> 1)

func (qs *QuadStore) indexOf(t quad.Quad) (int64, bool) {
	min := maxInt
	var tree *b.Tree
	for d := quad.Subject; d <= quad.Label; d++ {
		sid := t.Get(d)
		if d == quad.Label && sid == "" {
			continue
		}
		id, ok := qs.idMap[sid]
		// If we've never heard about a node, it must not exist
		if !ok {
			return 0, false
		}
		index, ok := qs.index.Get(d, id)
		if !ok {
			// If it's never been indexed in this direction, it can't exist.
			return 0, false
		}
		if l := index.Len(); l < min {
			min, tree = l, index
		}
	}
	it := NewIterator(tree, "", qs)

	for it.Next() {
		val := it.Result()
		if t == qs.log[val.(int64)].Quad {
			return val.(int64), true
		}
	}
	return 0, false
}

func (qs *QuadStore) AddDelta(d graph.Delta) error {
	if _, exists := qs.indexOf(d.Quad); exists {
		return graph.ErrQuadExists
	}
	qid := qs.nextQuadID
	qs.log = append(qs.log, LogEntry{
		ID:        d.ID.Int(),
		Quad:      d.Quad,
		Action:    d.Action,
		Timestamp: d.Timestamp})
	qs.size++
	qs.nextQuadID++

	for dir := quad.Subject; dir <= quad.Label; dir++ {
		sid := d.Quad.Get(dir)
		if dir == quad.Label && sid == "" {
			continue
		}
		if _, ok := qs.idMap[sid]; !ok {
			qs.idMap[sid] = qs.nextID
			qs.revIDMap[qs.nextID] = sid
			qs.nextID++
		}
		id := qs.idMap[sid]
		tree := qs.index.Tree(dir, id)
		tree.Set(qid, struct{}{})
	}

	// TODO(barakmich): Add VIP indexing
	return nil
}

func (qs *QuadStore) RemoveDelta(d graph.Delta) error {
	prevQuadID, exists := qs.indexOf(d.Quad)
	if !exists {
		return graph.ErrQuadNotExist
	}

	quadID := qs.nextQuadID
	qs.log = append(qs.log, LogEntry{
		ID:        d.ID.Int(),
		Quad:      d.Quad,
		Action:    d.Action,
		Timestamp: d.Timestamp})
	qs.log[prevQuadID].DeletedBy = quadID
	qs.size--
	qs.nextQuadID++
	return nil
}

func (qs *QuadStore) Quad(index graph.Value) quad.Quad {
	return qs.log[index.(int64)].Quad
}

func (qs *QuadStore) QuadIterator(d quad.Direction, value graph.Value) graph.Iterator {
	index, ok := qs.index.Get(d, value.(int64))
	data := fmt.Sprintf("dir:%s val:%d", d, value.(int64))
	if ok {
		return NewIterator(index, data, qs)
	}
	return &iterator.Null{}
}

func (qs *QuadStore) Horizon() graph.PrimaryKey {
	return graph.NewSequentialKey(qs.log[len(qs.log)-1].ID)
}

func (qs *QuadStore) Size() int64 {
	return qs.size
}

func (qs *QuadStore) DebugPrint() {
	for i, l := range qs.log {
		if i == 0 {
			continue
		}
		glog.V(2).Infof("%d: %#v", i, l)
	}
}

func (qs *QuadStore) ValueOf(name string) graph.Value {
	return qs.idMap[name]
}

func (qs *QuadStore) NameOf(id graph.Value) string {
	if id == nil {
		return ""
	}
	return qs.revIDMap[id.(int64)]
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return newQuadsAllIterator(qs)
}

func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(iterator.Identity)
}

func (qs *QuadStore) QuadDirection(val graph.Value, d quad.Direction) graph.Value {
	name := qs.Quad(val).Get(d)
	return qs.ValueOf(name)
}

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return newNodesAllIterator(qs)
}

func (qs *QuadStore) Close() {}

func (qs *QuadStore) Type() string {
	return QuadStoreType
}
