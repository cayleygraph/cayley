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

package bolt2

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/proto"
	"github.com/cayleygraph/cayley/quad"
)

var (
	constrainedAllType graph.Type
)

func init() {
	constrainedAllType = graph.RegisterIterator("bolt_constrained_all")
}

type AllIterator struct {
	nodes   bool
	id      uint64
	prim    *proto.Primitive
	horizon int64
	tags    graph.Tagger
	qs      *QuadStore
	err     error
	uid     uint64
	cons    *constraint
}

var _ graph.Iterator = &AllIterator{}

type constraint struct {
	dir quad.Direction
	val Int64Value
}

func NewAllIterator(nodes bool, qs *QuadStore, cons *constraint) *AllIterator {
	qs.mu.RLock()
	defer qs.mu.RUnlock()
	if nodes && cons != nil {
		panic("cannot use a bolt2 all iterator across nodes with a constraint")
	}
	return &AllIterator{
		nodes:   nodes,
		qs:      qs,
		horizon: qs.horizon,
		uid:     iterator.NextUID(),
		cons:    cons,
	}
}

func (it *AllIterator) UID() uint64 {
	return it.uid
}

func (it *AllIterator) Reset() {
	it.id = 0
}

func (it *AllIterator) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *AllIterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *AllIterator) Clone() graph.Iterator {
	out := NewAllIterator(it.nodes, it.qs, it.cons)
	out.tags.CopyFrom(it)
	return out
}

func (it *AllIterator) Close() error {
	return nil
}

func (it *AllIterator) Err() error {
	return it.err
}

func (it *AllIterator) Result() graph.Value {
	if it.id > uint64(it.horizon) {
		return nil
	}
	if it.nodes {
		return Int64Value(it.id)
	}
	return it.prim
}

// No subiterators.
func (it *AllIterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *AllIterator) Next() bool {
	for {
		it.id++
		if it.id > uint64(it.horizon) {
			return false
		}
		p, ok := it.qs.getPrimitive(Int64Value(it.id))
		it.prim = p
		if !ok {
			return false
		}
		if p.Deleted {
			continue
		}
		if p.IsNode() && it.nodes {
			return true
		}
		if !p.IsNode() && !it.nodes {
			if it.cons == nil {
				return true
			}
			if Int64Value(p.GetDirection(it.cons.dir)) == it.cons.val {
				return true
			}
		}
	}
}

func (it *AllIterator) NextPath() bool {
	return false
}

func (it *AllIterator) Contains(v graph.Value) bool {
	if it.nodes {
		x, ok := v.(Int64Value)
		if !ok {
			return false
		}
		it.id = uint64(x)
		return it.id <= uint64(it.horizon)
	}
	p, ok := v.(*proto.Primitive)
	if !ok {
		return false
	}
	it.prim = p
	it.id = it.prim.ID
	if it.cons == nil {
		return true
	}
	if Int64Value(it.prim.GetDirection(it.cons.dir)) != it.cons.val {
		return false
	}
	return true
}

func (it *AllIterator) Size() (int64, bool) {
	return it.qs.Size(), false
}

func (it *AllIterator) Describe() graph.Description {
	size, _ := it.Size()
	return graph.Description{
		UID:       it.UID(),
		Type:      it.Type(),
		Tags:      it.tags.Tags(),
		Size:      size,
		Direction: quad.Any,
	}
}

func (it *AllIterator) Type() graph.Type {
	if it.cons != nil {
		return constrainedAllType
	}
	return graph.All
}
func (it *AllIterator) Sorted() bool { return false }

func (it *AllIterator) Optimize() (graph.Iterator, bool) {
	return it, false
}

func (it *AllIterator) Stats() graph.IteratorStats {
	s, exact := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     2,
		Size:         s,
		ExactSize:    exact,
	}
}
