// Copyright 2018 The Cayley Authors. All rights reserved.
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

package iterator

import (
	"context"
	"fmt"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
)

var _ graph.Iterator = &Resolver{}

// A Resolver iterator consists of it's order, an index (where it is in the,
// process of iterating) and a store to resolve values from.
type Resolver struct {
	qs     graph.QuadStore
	uid    uint64
	tags   graph.Tagger
	order  []quad.Value
	values map[quad.Value]graph.Value
	nodes  map[interface{}]quad.Value
	cached bool
	index  int
	err    error
	result graph.Value
}

// Creates a new Resolver iterator.
func NewResolver(qs graph.QuadStore, nodes ...quad.Value) *Resolver {
	it := &Resolver{
		uid:   NextUID(),
		qs:    qs,
		order: make([]quad.Value, len(nodes)),
		// Generally there are going to be no/few duplicates given
		// so allocate maps large enough to accommodate all
		values: make(map[quad.Value]graph.Value, len(nodes)),
		nodes:  make(map[interface{}]quad.Value, len(nodes)),
	}
	copy(it.order, nodes)
	return it
}

func (it *Resolver) UID() uint64 {
	return it.uid
}

func (it *Resolver) Reset() {
	it.index = 0
	it.err = nil
	it.result = nil
}

func (it *Resolver) Close() error {
	return nil
}

func (it *Resolver) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *Resolver) TagResults(dst map[string]graph.Value) {
	it.tags.TagResult(dst, it.Result())
}

func (it *Resolver) Clone() graph.Iterator {
	out := NewResolver(it.qs, it.order...)
	// Nodes and values maps should contain identical data, so
	// just iterate through one
	for node, value := range it.values {
		out.values[node] = value
		out.nodes[value.Key()] = node
	}
	out.tags.CopyFrom(it)
	return out
}

func (it *Resolver) String() string {
	return fmt.Sprintf("Resolver(%v)", it.order)
}

// Register this iterator as a Resolver iterator.
func (it *Resolver) Type() graph.Type { return graph.Resolver }

// Resolve nodes to values
func (it *Resolver) resolve(ctx context.Context) error {
	values, err := graph.RefsOf(ctx, it.qs, it.order)
	if err != nil {
		return err
	}
	for index, value := range values {
		node := it.order[index]
		it.values[node] = value
		it.nodes[value.Key()] = node
	}
	it.cached = true
	return nil
}

// Check if the passed value is equal to one of the order stored in the iterator.
func (it *Resolver) Contains(ctx context.Context, value graph.Value) bool {
	graph.ContainsLogIn(it, value)
	if !it.cached {
		it.err = it.resolve(ctx)
		if it.err != nil {
			return false
		}
	}
	_, ok := it.nodes[value.Key()]
	return graph.ContainsLogOut(it, value, ok)
}

// Next advances the iterator.
func (it *Resolver) Next(ctx context.Context) bool {
	graph.NextLogIn(it)
	if it.index >= len(it.order) {
		it.result = nil
		return graph.NextLogOut(it, false)
	}
	if !it.cached {
		it.err = it.resolve(ctx)
		if it.err != nil {
			return false
		}
	}
	node := it.order[it.index]
	value, ok := it.values[node]
	if !ok {
		it.result = nil
		it.err = fmt.Errorf("not found: %v", node)
		return graph.NextLogOut(it, false)
	}
	it.result = value
	it.index++
	return graph.NextLogOut(it, true)
}

func (it *Resolver) Err() error {
	return it.err
}

func (it *Resolver) Result() graph.Value {
	return it.result
}

func (it *Resolver) NextPath(ctx context.Context) bool {
	return false
}

func (it *Resolver) SubIterators() []graph.Iterator {
	return nil
}

// Returns a Null iterator if it's empty so that upstream iterators can optimize it
// away, otherwise there is no optimization.
func (it *Resolver) Optimize() (graph.Iterator, bool) {
	if len(it.order) == 0 {
		return NewNull(), true
	}
	return it, false
}

// Size is the number of m stored.
func (it *Resolver) Size() (int64, bool) {
	return int64(len(it.order)), true
}

func (it *Resolver) Stats() graph.IteratorStats {
	s, exact := it.Size()
	return graph.IteratorStats{
		// Lookup cost is size of set
		ContainsCost: s,
		// Next is (presumably) O(1) from store
		NextCost:  1,
		Size:      s,
		ExactSize: exact,
	}
}
