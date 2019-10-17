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
	"context"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/proto"
	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/quad"
)

type constraint struct {
	dir quad.Direction
	val Int64Value
}

type allIterator struct {
	qs    *QuadStore
	nodes bool
	cons  *constraint
}

func (qs *QuadStore) newAllIterator(nodes bool, cons *constraint) *allIterator {
	if nodes && cons != nil {
		panic("cannot use a kv all iterator across nodes with a constraint")
	}
	return &allIterator{
		qs:    qs,
		nodes: nodes,
		cons:  cons,
	}
}

func (it *allIterator) Iterate() iterator.Scanner {
	return it.qs.newAllIteratorNext(it.nodes, it.cons)
}

func (it *allIterator) Lookup() iterator.Index {
	return it.qs.newAllIteratorContains(it.nodes, it.cons)
}

// No subiterators.
func (it *allIterator) SubIterators() []iterator.Shape {
	return nil
}

func (it *allIterator) String() string {
	return "KVAll"
}

func (it *allIterator) Sorted() bool { return false }

func (it *allIterator) Optimize(ctx context.Context) (iterator.Shape, bool) {
	return it, false
}

func (it *allIterator) Stats(ctx context.Context) (iterator.Costs, error) {
	return iterator.Costs{
		ContainsCost: 1,
		NextCost:     2,
		Size: refs.Size{
			Value: it.qs.Size(),
			Exact: false,
		},
	}, nil
}

type allIteratorNext struct {
	nodes   bool
	id      uint64
	buf     []*proto.Primitive
	prim    *proto.Primitive
	horizon int64
	qs      *QuadStore
	err     error
	cons    *constraint
}

func (qs *QuadStore) newAllIteratorNext(nodes bool, cons *constraint) *allIteratorNext {
	if nodes && cons != nil {
		panic("cannot use a kv all iterator across nodes with a constraint\n")
	}
	return &allIteratorNext{
		qs:      qs,
		nodes:   nodes,
		horizon: qs.horizon(context.TODO()),
		cons:    cons,
	}
}

func (it *allIteratorNext) TagResults(dst map[string]graph.Ref) {}

func (it *allIteratorNext) Close() error {
	return nil
}

func (it *allIteratorNext) Err() error {
	return it.err
}

func (it *allIteratorNext) Result() graph.Ref {
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

const nextBatch = 100

func (it *allIteratorNext) Next(ctx context.Context) bool {
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
			it.buf, it.err = it.qs.getPrimitives(ctx, ids)
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

func (it *allIteratorNext) NextPath(ctx context.Context) bool {
	return false
}

func (it *allIteratorNext) String() string {
	return "KVAllNext"
}

func (it *allIteratorNext) Sorted() bool { return false }

type allIteratorContains struct {
	nodes   bool
	id      uint64
	prim    *proto.Primitive
	horizon int64
	qs      *QuadStore
	err     error
	cons    *constraint
}

func (qs *QuadStore) newAllIteratorContains(nodes bool, cons *constraint) *allIteratorContains {
	if nodes && cons != nil {
		panic("cannot use a kv all iterator across nodes with a constraint")
	}
	return &allIteratorContains{
		qs:      qs,
		nodes:   nodes,
		horizon: qs.horizon(context.TODO()),
		cons:    cons,
	}
}

func (it *allIteratorContains) TagResults(dst map[string]graph.Ref) {}

func (it *allIteratorContains) Close() error {
	return nil
}

func (it *allIteratorContains) Err() error {
	return it.err
}

func (it *allIteratorContains) Result() graph.Ref {
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

func (it *allIteratorContains) NextPath(ctx context.Context) bool {
	return false
}

func (it *allIteratorContains) Contains(ctx context.Context, v graph.Ref) bool {
	// TODO(dennwc): This method doesn't check if the primitive still exists in the store.
	//               It's okay if we assume we provide the snapshot of data, though.
	//               However, passing a hand-crafted Ref will cause invalid results.
	//               Same is true for QuadIterator.
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

func (it *allIteratorContains) String() string {
	return "KVAllContains"
}

func (it *allIteratorContains) Sorted() bool { return false }
