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

// Defines one of the base iterators, the All iterator. Which, logically
// enough, represents all nodes or all links in the graph.
//
// This particular file is actually vestigial. It's up to the QuadStore to give
// us an All iterator that represents all things in the graph. So this is
// really the All iterator for the memstore.QuadStore. That said, it *is* one of
// the base iterators, and it helps just to see it here.

import (
	"github.com/cayleygraph/cayley/graph"
)

// An All iterator across a range of int64 values, from `max` to `min`.
type Int64 struct {
	node     bool
	uid      uint64
	tags     graph.Tagger
	max, min int64
	at       int64
	result   int64
	runstats graph.IteratorStats
}

type Int64Node int64

func (Int64Node) IsNode() bool { return true }

type Int64Quad int64

func (Int64Quad) IsNode() bool { return false }

// Creates a new Int64 with the given range.
func NewInt64(min, max int64, node bool) *Int64 {
	return &Int64{
		uid:  NextUID(),
		node: node,
		min:  min,
		max:  max,
		at:   min,
	}
}

func (it *Int64) UID() uint64 {
	return it.uid
}

// Start back at the beginning
func (it *Int64) Reset() {
	it.at = it.min
}

func (it *Int64) Close() error {
	return nil
}

func (it *Int64) Clone() graph.Iterator {
	out := NewInt64(it.min, it.max, it.node)
	out.tags.CopyFrom(it)
	return out
}

func (it *Int64) Tagger() *graph.Tagger {
	return &it.tags
}

// Fill the map based on the tags assigned to this iterator.
func (it *Int64) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *Int64) Describe() graph.Description {
	return graph.Description{
		UID:  it.UID(),
		Type: it.Type(),
		Tags: it.tags.Tags(),
	}
}

// Next() on an Int64 all iterator is a simple incrementing counter.
// Return the next integer, and mark it as the result.
func (it *Int64) Next() bool {
	graph.NextLogIn(it)
	it.runstats.Next += 1
	if it.at == -1 {
		return graph.NextLogOut(it, false)
	}
	val := it.at
	it.at = it.at + 1
	if it.at > it.max {
		it.at = -1
	}
	it.result = val
	return graph.NextLogOut(it, true)
}

func (it *Int64) Err() error {
	return nil
}

func (it *Int64) toValue(v int64) graph.Value {
	if it.node {
		return Int64Node(v)
	}
	return Int64Quad(v)
}

func (it *Int64) Result() graph.Value {
	return it.toValue(it.result)
}

func (it *Int64) NextPath() bool {
	return false
}

// No sub-iterators.
func (it *Int64) SubIterators() []graph.Iterator {
	return nil
}

// The number of elements in an Int64 is the size of the range.
// The size is exact.
func (it *Int64) Size() (int64, bool) {
	Size := ((it.max - it.min) + 1)
	return Size, true
}

// Contains() for an Int64 is merely seeing if the passed value is
// within the range, assuming the value is an int64.
func (it *Int64) Contains(tsv graph.Value) bool {
	graph.ContainsLogIn(it, tsv)
	it.runstats.Contains += 1
	var v int64
	if tsv.IsNode() {
		v = int64(tsv.(Int64Node))
	} else {
		v = int64(tsv.(Int64Quad))
	}
	if it.min <= v && v <= it.max {
		it.result = v
		return graph.ContainsLogOut(it, it.toValue(v), true)
	}
	return graph.ContainsLogOut(it, it.toValue(v), false)
}

// The type of this iterator is an "all". This is important, as it puts it in
// the class of "all iterators.
func (it *Int64) Type() graph.Type { return graph.All }

// There's nothing to optimize about this little iterator.
func (it *Int64) Optimize() (graph.Iterator, bool) { return it, false }

// Stats for an Int64 are simple. Super cheap to do any operation,
// and as big as the range.
func (it *Int64) Stats() graph.IteratorStats {
	s, exact := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     1,
		Size:         s,
		ExactSize:    exact,
		Next:         it.runstats.Next,
		Contains:     it.runstats.Contains,
	}
}

var _ graph.Iterator = &Int64{}
