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
	"github.com/cayleygraph/cayley/quad"
)

var _ graph.Iterator = &Iterator{}

type Iterator struct {
	nodes bool
	uid   uint64
	qs    *QuadStore
	tree  *Tree

	iter *Enumerator
	cur  *primitive
	err  error

	d     quad.Direction
	value int64
}

func NewIterator(tree *Tree, qs *QuadStore, d quad.Direction, value int64) *Iterator {
	return &Iterator{
		nodes: d == 0,
		uid:   iterator.NextUID(),
		qs:    qs,
		tree:  tree,
		d:     d,
		value: value,
	}
}

func (it *Iterator) UID() uint64 {
	return it.uid
}

func (it *Iterator) Reset() {
	it.iter = nil
	it.err = nil
	it.cur = nil
}

func (it *Iterator) TagResults(dst map[string]graph.Value) {}

func (it *Iterator) Close() error {
	return nil
}

func (it *Iterator) Next(ctx context.Context) bool {
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

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Result() graph.Value {
	if it.cur == nil {
		return nil
	}
	return qprim{p: it.cur}
}

func (it *Iterator) NextPath(ctx context.Context) bool {
	return false
}

// No subiterators.
func (it *Iterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *Iterator) Size() (int64, bool) {
	return int64(it.tree.Len()), true
}

func (it *Iterator) Contains(ctx context.Context, v graph.Value) bool {
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

func (it *Iterator) String() string {
	return fmt.Sprintf("MemStore(%v)", it.d)
}

func (it *Iterator) Sorted() bool { return true }

func (it *Iterator) Optimize() (graph.Iterator, bool) {
	return it, false
}

func (it *Iterator) Stats() graph.IteratorStats {
	return graph.IteratorStats{
		ContainsCost: int64(math.Log(float64(it.tree.Len()))) + 1,
		NextCost:     1,
		Size:         int64(it.tree.Len()),
		ExactSize:    true,
	}
}
