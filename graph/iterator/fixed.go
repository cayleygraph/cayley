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
// A fixed iterator requires an Equality function to be passed to it, by reason that graph.Value, the
// opaque Quad store value, may not answer to ==.

import (
	"context"
	"fmt"

	"github.com/cayleygraph/cayley/graph"
)

var _ graph.Iterator = &Fixed{}

// A Fixed iterator consists of it's values, an index (where it is in the process of Next()ing) and
// an equality function.
type Fixed struct {
	uid       uint64
	values    []graph.Value
	lastIndex int
	result    graph.Value
}

// Creates a new Fixed iterator with a custom comparator.
func NewFixed(vals ...graph.Value) *Fixed {
	it := &Fixed{
		uid:    NextUID(),
		values: make([]graph.Value, 0, 20),
	}
	for _, v := range vals {
		it.Add(v)
	}
	return it
}

func (it *Fixed) UID() uint64 {
	return it.uid
}

func (it *Fixed) Reset() {
	it.lastIndex = 0
}

func (it *Fixed) Close() error {
	return nil
}

func (it *Fixed) TagResults(dst map[string]graph.Value) {}

func (it *Fixed) Clone() graph.Iterator {
	return NewFixed(it.values...)
}

// Add a value to the iterator. The array now contains this value.
// TODO(barakmich): This ought to be a set someday, disallowing repeated values.
func (it *Fixed) Add(v graph.Value) {
	it.values = append(it.values, v)
}

// Values returns a list of values stored in iterator. Slice should not be modified.
func (it *Fixed) Values() []graph.Value {
	return it.values
}

func (it *Fixed) String() string {
	return fmt.Sprintf("Fixed(%v)", it.values)
}

// Register this iterator as a Fixed iterator.
func (it *Fixed) Type() graph.Type { return graph.Fixed }

// Check if the passed value is equal to one of the values stored in the iterator.
func (it *Fixed) Contains(ctx context.Context, v graph.Value) bool {
	// Could be optimized by keeping it sorted or using a better datastructure.
	// However, for fixed iterators, which are by definition kind of tiny, this
	// isn't a big issue.
	vk := graph.ToKey(v)
	for _, x := range it.values {
		if graph.ToKey(x) == vk {
			it.result = x
			return true
		}
	}
	return false
}

// Next advances the iterator.
func (it *Fixed) Next(ctx context.Context) bool {
	if it.lastIndex == len(it.values) {
		return false
	}
	out := it.values[it.lastIndex]
	it.result = out
	it.lastIndex++
	return true
}

func (it *Fixed) Err() error {
	return nil
}

func (it *Fixed) Result() graph.Value {
	return it.result
}

func (it *Fixed) NextPath(ctx context.Context) bool {
	return false
}

// No sub-iterators.
func (it *Fixed) SubIterators() []graph.Iterator {
	return nil
}

// Optimize() for a Fixed iterator is simple. Returns a Null iterator if it's empty
// (so that other iterators upstream can treat this as null) or there is no
// optimization.
func (it *Fixed) Optimize() (graph.Iterator, bool) {
	if len(it.values) == 1 && it.values[0] == nil {
		return NewNull(), true
	}

	return it, false
}

// Size is the number of values stored.
func (it *Fixed) Size() (int64, bool) {
	return int64(len(it.values)), true
}

// As we right now have to scan the entire list, Next and Contains are linear with the
// size. However, a better data structure could remove these limits.
func (it *Fixed) Stats() graph.IteratorStats {
	s, exact := it.Size()
	return graph.IteratorStats{
		ContainsCost: s,
		NextCost:     s,
		Size:         s,
		ExactSize:    exact,
	}
}
