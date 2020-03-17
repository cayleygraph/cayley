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
	"context"
	"fmt"
	"io"
	"math"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/quad"
)

type Iterator struct {
	qs    *QuadStore
	tree  *Tree
	d     quad.Direction
	value int64
}

func (qs *QuadStore) newIterator(tree *Tree, d quad.Direction, value int64) *Iterator {
	return &Iterator{
		qs:    qs,
		tree:  tree,
		d:     d,
		value: value,
	}
}

func (it *Iterator) Iterate() iterator.Scanner {
	// TODO(dennwc): it doesn't check the direction and value, while Contains does; is it expected?
	return it.qs.newIteratorNext(it.tree, it.d)
}

func (it *Iterator) Lookup() iterator.Index {
	return it.qs.newIteratorContains(it.tree, it.d, it.value)
}

func (it *Iterator) SubIterators() []iterator.Shape {
	return nil
}

func (it *Iterator) String() string {
	return fmt.Sprintf("MemStore(%v)", it.d)
}

func (it *Iterator) Sorted() bool { return true }

func (it *Iterator) Optimize(ctx context.Context) (iterator.Shape, bool) {
	return it, false
}

func (it *Iterator) Stats(ctx context.Context) (iterator.Costs, error) {
	return iterator.Costs{
		ContainsCost: int64(math.Log(float64(it.tree.Len()))) + 1,
		NextCost:     1,
		Size: refs.Size{
			Value: int64(it.tree.Len()),
			Exact: true,
		},
	}, nil
}

type iteratorNext struct {
	nodes bool
	qs    *QuadStore
	tree  *Tree
	d     quad.Direction

	iter *Enumerator
	cur  *Primitive
	err  error
}

func (qs *QuadStore) newIteratorNext(tree *Tree, d quad.Direction) *iteratorNext {
	return &iteratorNext{
		nodes: d == 0,
		d:     d,
		qs:    qs,
		tree:  tree,
	}
}

func (it *iteratorNext) TagResults(dst map[string]graph.Ref) {}

func (it *iteratorNext) Close() error {
	return nil
}

func (it *iteratorNext) Next(ctx context.Context) bool {
	if it.iter == nil {
		it.iter, it.err = it.tree.SeekFirst()
		if it.err == io.EOF || it.iter == nil {
			it.err = nil
			return false
		} else if it.err != nil {
			return false
		}
	}
	for {
		_, p, err := it.iter.Next()
		if err != nil {
			if err != io.EOF {
				it.err = err
			}
			return false
		}
		it.cur = p
		return true
	}
}

func (it *iteratorNext) Err() error {
	return it.err
}

func (it *iteratorNext) Result() graph.Ref {
	if it.cur == nil {
		return nil
	}
	return qprim{p: it.cur}
}

func (it *iteratorNext) NextPath(ctx context.Context) bool {
	return false
}

func (it *iteratorNext) String() string {
	return fmt.Sprintf("MemStoreNext(%v)", it.d)
}

func (it *iteratorNext) Sorted() bool { return true }

type iteratorContains struct {
	nodes bool
	qs    *QuadStore
	tree  *Tree

	cur *Primitive

	d     quad.Direction
	value int64
}

func (qs *QuadStore) newIteratorContains(tree *Tree, d quad.Direction, value int64) *iteratorContains {
	return &iteratorContains{
		nodes: d == 0,
		qs:    qs,
		tree:  tree,
		d:     d,
		value: value,
	}
}

func (it *iteratorContains) TagResults(dst map[string]graph.Ref) {}

func (it *iteratorContains) Close() error {
	return nil
}

func (it *iteratorContains) Err() error {
	return nil
}

func (it *iteratorContains) Result() graph.Ref {
	if it.cur == nil {
		return nil
	}
	return qprim{p: it.cur}
}

func (it *iteratorContains) NextPath(ctx context.Context) bool {
	return false
}

func (it *iteratorContains) Contains(ctx context.Context, v graph.Ref) bool {
	if v == nil {
		return false
	}
	switch v := v.(type) {
	case bnode:
		if p, ok := it.tree.Get(int64(v)); ok {
			it.cur = p
			return true
		}
	case qprim:
		if v.p.Quad.Dir(it.d) == it.value {
			it.cur = v.p
			return true
		}
	}
	return false
}

func (it *iteratorContains) String() string {
	return fmt.Sprintf("MemStoreContains(%v)", it.d)
}

func (it *iteratorContains) Sorted() bool { return true }
