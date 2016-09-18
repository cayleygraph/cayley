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
	"fmt"
	"sort"

	"github.com/cayleygraph/cayley/graph"
)

// A Fixed iterator consists of it's values, an index (where it is in the process of Next()ing) and
// an equality function.
type Fixed struct {
	uid       uint64
	tags      graph.Tagger
	values    []graph.Value
	lastIndex int
	cmp       Equality
	result    graph.Value
}

// Define the signature of an equality function.
type Equality func(a, b graph.Value) bool

// Define an equality function of purely ==, which works for native types.
func Identity(a, b graph.Value) bool {
	return a == b
}

// Creates a new Fixed iterator with a custom comparator.
func NewFixed(cmp Equality, vals ...graph.Value) *Fixed {
	it := &Fixed{
		uid:    NextUID(),
		values: make([]graph.Value, 0, 20),
		cmp:    cmp,
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

func (it *Fixed) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *Fixed) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *Fixed) Clone() graph.Iterator {
	vals := make([]graph.Value, len(it.values))
	copy(vals, it.values)
	out := NewFixed(it.cmp, vals...)
	out.tags.CopyFrom(it)
	return out
}

// Add a value to the iterator. The array now contains this value.
// TODO(barakmich): This ought to be a set someday, disallowing repeated values.
func (it *Fixed) Add(v graph.Value) {
	it.values = append(it.values, v)
}

func (it *Fixed) Describe() graph.Description {
	var value string
	if len(it.values) > 0 {
		value = fmt.Sprint(it.values[0])
	}
	fixed := make([]string, 0, len(it.tags.Fixed()))
	for k := range it.tags.Fixed() {
		fixed = append(fixed, k)
	}
	sort.Strings(fixed)
	return graph.Description{
		UID:  it.UID(),
		Name: value,
		Type: it.Type(),
		Tags: fixed,
		Size: int64(len(it.values)),
	}
}

// Register this iterator as a Fixed iterator.
func (it *Fixed) Type() graph.Type { return graph.Fixed }

// Check if the passed value is equal to one of the values stored in the iterator.
func (it *Fixed) Contains(v graph.Value) bool {
	// Could be optimized by keeping it sorted or using a better datastructure.
	// However, for fixed iterators, which are by definition kind of tiny, this
	// isn't a big issue.
	graph.ContainsLogIn(it, v)
	for _, x := range it.values {
		if it.cmp(x, v) {
			it.result = x
			return graph.ContainsLogOut(it, v, true)
		}
	}
	return graph.ContainsLogOut(it, v, false)
}

// Next advances the iterator.
func (it *Fixed) Next() bool {
	graph.NextLogIn(it)
	if it.lastIndex == len(it.values) {
		return graph.NextLogOut(it, false)
	}
	out := it.values[it.lastIndex]
	it.result = out
	it.lastIndex++
	return graph.NextLogOut(it, true)
}

func (it *Fixed) Err() error {
	return nil
}

func (it *Fixed) Result() graph.Value {
	return it.result
}

func (it *Fixed) NextPath() bool {
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
		return &Null{}, true
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

var _ graph.Iterator = &Fixed{}
