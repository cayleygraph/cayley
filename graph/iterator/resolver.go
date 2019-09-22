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
	"github.com/cayleygraph/quad"
)

var _ graph.IteratorFuture = &Resolver{}

// A Resolver iterator consists of it's order, an index (where it is in the,
// process of iterating) and a store to resolve values from.
type Resolver struct {
	it *resolver
	graph.Iterator
}

// Creates a new Resolver iterator.
func NewResolver(qs graph.QuadStore, nodes ...quad.Value) *Resolver {
	it := &Resolver{
		it: newResolver(qs, nodes...),
	}
	it.Iterator = graph.NewLegacy(it.it, it)
	return it
}

func (it *Resolver) AsShape() graph.IteratorShape {
	it.Close()
	return it.it
}

var _ graph.IteratorShapeCompat = (*resolver)(nil)

// A Resolver iterator consists of it's order, an index (where it is in the,
// process of iterating) and a store to resolve values from.
type resolver struct {
	qs    graph.QuadStore
	order []quad.Value
}

// Creates a new Resolver iterator.
func newResolver(qs graph.QuadStore, nodes ...quad.Value) *resolver {
	it := &resolver{
		qs:    qs,
		order: make([]quad.Value, len(nodes)),
	}
	copy(it.order, nodes)
	return it
}

func (it *resolver) Iterate() graph.Scanner {
	return newResolverNext(it.qs, it.order)
}

func (it *resolver) Lookup() graph.Index {
	return newResolverContains(it.qs, it.order)
}

func (it *resolver) AsLegacy() graph.Iterator {
	it2 := &Resolver{it: it}
	it2.Iterator = graph.NewLegacy(it, it2)
	return it2
}

func (it *resolver) String() string {
	return fmt.Sprintf("Resolver(%v)", it.order)
}

func (it *resolver) SubIterators() []graph.IteratorShape {
	return nil
}

// Returns a Null iterator if it's empty so that upstream iterators can optimize it
// away, otherwise there is no optimization.
func (it *resolver) Optimize(ctx context.Context) (graph.IteratorShape, bool) {
	if len(it.order) == 0 {
		return newNull(), true
	}
	return it, false
}

func (it *resolver) Stats(ctx context.Context) (graph.IteratorCosts, error) {
	return graph.IteratorCosts{
		// Next is (presumably) O(1) from store
		NextCost:     1,
		ContainsCost: 1,
		Size: graph.Size{
			Size:  int64(len(it.order)),
			Exact: true,
		},
	}, nil
}

// A Resolver iterator consists of it's order, an index (where it is in the,
// process of iterating) and a store to resolve values from.
type resolverNext struct {
	qs     graph.QuadStore
	order  []quad.Value
	values []graph.Ref
	cached bool
	index  int
	err    error
	result graph.Ref
}

// Creates a new Resolver iterator.
func newResolverNext(qs graph.QuadStore, nodes []quad.Value) *resolverNext {
	it := &resolverNext{
		qs:    qs,
		order: make([]quad.Value, len(nodes)),
	}
	copy(it.order, nodes)
	return it
}

func (it *resolverNext) Close() error {
	return nil
}

func (it *resolverNext) TagResults(dst map[string]graph.Ref) {}

func (it *resolverNext) String() string {
	return fmt.Sprintf("ResolverNext(%v, %v)", it.order, it.values)
}

// Resolve nodes to values
func (it *resolverNext) resolve(ctx context.Context) error {
	values, err := graph.RefsOf(ctx, it.qs, it.order)
	if err != nil {
		return err
	}
	it.values = make([]graph.Ref, len(it.order))
	for i, value := range values {
		it.values[i] = value
	}
	it.order = nil
	it.cached = true
	return nil
}

// Next advances the iterator.
func (it *resolverNext) Next(ctx context.Context) bool {
	if !it.cached {
		it.err = it.resolve(ctx)
		if it.err != nil {
			return false
		}
	}
	if it.index >= len(it.values) {
		it.result = nil
		return false
	}
	it.result = it.values[it.index]
	it.index++
	return true
}

func (it *resolverNext) Err() error {
	return it.err
}

func (it *resolverNext) Result() graph.Ref {
	return it.result
}

func (it *resolverNext) NextPath(ctx context.Context) bool {
	return false
}

// A Resolver iterator consists of it's order, an index (where it is in the,
// process of iterating) and a store to resolve values from.
type resolverContains struct {
	qs     graph.QuadStore
	order  []quad.Value
	nodes  map[interface{}]quad.Value
	cached bool
	err    error
	result graph.Ref
}

// Creates a new Resolver iterator.
func newResolverContains(qs graph.QuadStore, nodes []quad.Value) *resolverContains {
	it := &resolverContains{
		qs:    qs,
		order: make([]quad.Value, len(nodes)),
	}
	copy(it.order, nodes)
	return it
}

func (it *resolverContains) Close() error {
	return nil
}

func (it *resolverContains) TagResults(dst map[string]graph.Ref) {}

func (it *resolverContains) String() string {
	return fmt.Sprintf("ResolverContains(%v, %v)", it.order, it.nodes)
}

// Resolve nodes to values
func (it *resolverContains) resolve(ctx context.Context) error {
	values, err := graph.RefsOf(ctx, it.qs, it.order)
	if err != nil {
		return err
	}
	// Generally there are going to be no/few duplicates given
	// so allocate maps large enough to accommodate all
	it.nodes = make(map[interface{}]quad.Value, len(it.order))
	for index, value := range values {
		node := it.order[index]
		it.nodes[value.Key()] = node
	}
	it.order = nil
	it.cached = true
	return nil
}

// Check if the passed value is equal to one of the order stored in the iterator.
func (it *resolverContains) Contains(ctx context.Context, value graph.Ref) bool {
	if !it.cached {
		it.err = it.resolve(ctx)
		if it.err != nil {
			return false
		}
	}
	_, ok := it.nodes[value.Key()]
	if ok {
		it.result = value
	}
	return ok
}

func (it *resolverContains) Err() error {
	return it.err
}

func (it *resolverContains) Result() graph.Ref {
	return it.result
}

func (it *resolverContains) NextPath(ctx context.Context) bool {
	return false
}
