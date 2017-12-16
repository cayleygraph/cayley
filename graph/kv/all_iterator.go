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

package kv

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/proto"
	"github.com/cayleygraph/cayley/quad"
)

type AllIterator struct {
	nodes   bool
	id      uint64
	buf     []*proto.Primitive
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
	if nodes && cons != nil {
		panic("cannot use a kv all iterator across nodes with a constraint")
	}
	return &AllIterator{
		nodes:   nodes,
		qs:      qs,
		horizon: qs.horizon(),
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
	it.tags.TagResult(dst, it.Result())
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
	if it.prim == nil {
		return nil
	}
	return it.prim
}

// No subiterators.
func (it *AllIterator) SubIterators() []graph.Iterator {
	return nil
}

const nextBatch = 100

func (it *AllIterator) Next() bool {
	if it.err != nil {
		return false
	}
	for {
		if len(it.buf) == 0 {
			if it.id+1 > uint64(it.horizon) {
				return false
			}
			ids := make([]uint64, 0, nextBatch)
			for i := 0; i < nextBatch; i++ {
				it.id++
				if it.id > uint64(it.horizon) {
					break
				}
				ids = append(ids, it.id)
			}
			if len(ids) == 0 {
				return false
			}
			it.buf, it.err = it.qs.getPrimitives(ids)
			if it.err != nil || len(it.buf) == 0 {
				return false
			}
		} else {
			it.buf = it.buf[1:]
		}
		for ; len(it.buf) > 0; it.buf = it.buf[1:] {
			p := it.buf[0]
			it.prim = p
			if p == nil || p.Deleted {
				continue
			}
			it.id = it.prim.ID
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

func (it *AllIterator) String() string {
	return "KVAll"
}

func (it *AllIterator) Type() graph.Type {
	if it.cons != nil {
		return "kv_constrained_all"
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
