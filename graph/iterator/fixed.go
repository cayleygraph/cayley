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

package iterator

// Defines one of the base iterators, the Fixed iterator. A fixed iterator is quite simple; it
// contains an explicit fixed array of values.
//
// A fixed iterator requires an Equality function to be passed to it, by reason that graph.Ref, the
// opaque Quad store value, may not answer to ==.

import (
	"context"
	"fmt"

	"github.com/cayleygraph/cayley/graph"
)

var _ graph.IteratorFuture = &Fixed{}

// A Fixed iterator consists of it's values, an index (where it is in the process of Next()ing) and
// an equality function.
type Fixed struct {
	it *fixed
	graph.Iterator
}

// Creates a new Fixed iterator with a custom comparator.
func NewFixed(vals ...graph.Ref) *Fixed {
	it := &Fixed{
		it: newFixed(vals...),
	}
	it.Iterator = graph.AsLegacy(it.it)
	return it
}

// Add a value to the iterator. The array now contains this value.
// TODO(barakmich): This ought to be a set someday, disallowing repeated values.
func (it *Fixed) Add(v graph.Ref) {
	it.it.Add(v)
}

// Values returns a list of values stored in iterator. Slice should not be modified.
func (it *Fixed) Values() []graph.Ref {
	return it.it.Values()
}

func (it *Fixed) AsShape() graph.IteratorShape {
	it.Close()
	return it.it
}

var _ graph.IteratorShape = &fixed{}

// A Fixed iterator consists of it's values, an index (where it is in the process of Next()ing) and
// an equality function.
type fixed struct {
	values []graph.Ref
}

// Creates a new Fixed iterator with a custom comparator.
func newFixed(vals ...graph.Ref) *fixed {
	return &fixed{
		values: append([]graph.Ref{}, vals...),
	}
}

func (it *fixed) Iterate() graph.Scanner {
	return newFixedNext(it.values)
}

func (it *fixed) Lookup() graph.Index {
	return newFixedContains(it.values)
}

func (it *fixed) AsLegacy() graph.Iterator {
	it2 := &Fixed{it: it}
	it2.Iterator = graph.NewLegacy(it, it2)
	return it2
}

// Add a value to the iterator. The array now contains this value.
// TODO(barakmich): This ought to be a set someday, disallowing repeated values.
func (it *fixed) Add(v graph.Ref) {
	it.values = append(it.values, v)
}

// Values returns a list of values stored in iterator. Slice must not be modified.
func (it *fixed) Values() []graph.Ref {
	return it.values
}

func (it *fixed) String() string {
	return fmt.Sprintf("Fixed(%v)", it.values)
}

// No sub-iterators.
func (it *fixed) SubIterators() []graph.IteratorShape {
	return nil
}

// Optimize() for a Fixed iterator is simple. Returns a Null iterator if it's empty
// (so that other iterators upstream can treat this as null) or there is no
// optimization.
func (it *fixed) Optimize(ctx context.Context) (graph.IteratorShape, bool) {
	if len(it.values) == 1 && it.values[0] == nil {
		return newNull(), true
	}

	return it, false
}

// As we right now have to scan the entire list, Next and Contains are linear with the
// size. However, a better data structure could remove these limits.
func (it *fixed) Stats(ctx context.Context) (graph.IteratorCosts, error) {
	return graph.IteratorCosts{
		ContainsCost: 1,
		NextCost:     1,
		Size: graph.Size{
			Size:  int64(len(it.values)),
			Exact: true,
		},
	}, nil
}

// A Fixed iterator consists of it's values, an index (where it is in the process of Next()ing) and
// an equality function.
type fixedNext struct {
	values []graph.Ref
	ind    int
	result graph.Ref
}

// Creates a new Fixed iterator with a custom comparator.
func newFixedNext(vals []graph.Ref) *fixedNext {
	return &fixedNext{
		values: vals,
	}
}

func (it *fixedNext) Close() error {
	return nil
}

func (it *fixedNext) TagResults(dst map[string]graph.Ref) {}

func (it *fixedNext) String() string {
	return fmt.Sprintf("Fixed(%v)", it.values)
}

// Next advances the iterator.
func (it *fixedNext) Next(ctx context.Context) bool {
	if it.ind >= len(it.values) {
		return false
	}
	out := it.values[it.ind]
	it.result = out
	it.ind++
	return true
}

func (it *fixedNext) Err() error {
	return nil
}

func (it *fixedNext) Result() graph.Ref {
	return it.result
}

func (it *fixedNext) NextPath(ctx context.Context) bool {
	return false
}

// A Fixed iterator consists of it's values, an index (where it is in the process of Next()ing) and
// an equality function.
type fixedContains struct {
	values []graph.Ref
	keys   []interface{}
	result graph.Ref
}

// Creates a new Fixed iterator with a custom comparator.
func newFixedContains(vals []graph.Ref) *fixedContains {
	keys := make([]interface{}, 0, len(vals))
	for _, v := range vals {
		keys = append(keys, graph.ToKey(v))
	}
	return &fixedContains{
		values: vals,
		keys:   keys,
	}
}

func (it *fixedContains) Close() error {
	return nil
}

func (it *fixedContains) TagResults(dst map[string]graph.Ref) {}

func (it *fixedContains) String() string {
	return fmt.Sprintf("Fixed(%v)", it.values)
}

// Check if the passed value is equal to one of the values stored in the iterator.
func (it *fixedContains) Contains(ctx context.Context, v graph.Ref) bool {
	// Could be optimized by keeping it sorted or using a better datastructure.
	// However, for fixed iterators, which are by definition kind of tiny, this
	// isn't a big issue.
	vk := graph.ToKey(v)
	for i, x := range it.keys {
		if x == vk {
			it.result = it.values[i]
			return true
		}
	}
	return false
}

func (it *fixedContains) Err() error {
	return nil
}

func (it *fixedContains) Result() graph.Ref {
	return it.result
}

func (it *fixedContains) NextPath(ctx context.Context) bool {
	return false
}
