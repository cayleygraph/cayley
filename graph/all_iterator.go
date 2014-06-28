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

package graph

// Defines one of the base iterators, the All iterator. Which, logically
// enough, represents all nodes or all links in the graph.
//
// This particular file is actually vestigal. It's up to the TripleStore to give
// us an All iterator that represents all things in the graph. So this is
// really the All iterator for the MemTripleStore. That said, it *is* one of
// the base iterators, and it helps just to see it here.

import (
	"fmt"
	"strings"
)

// An All iterator across a range of int64 values, from `max` to `min`.
type Int64AllIterator struct {
	BaseIterator
	max, min int64
	at       int64
}

// Creates a new Int64AllIterator with the given range.
func NewInt64AllIterator(min, max int64) *Int64AllIterator {
	var all Int64AllIterator
	BaseIteratorInit(&all.BaseIterator)
	all.max = max
	all.min = min
	all.at = min
	return &all
}

// Start back at the beginning
func (it *Int64AllIterator) Reset() {
	it.at = it.min
}

func (it *Int64AllIterator) Close() {}

func (it *Int64AllIterator) Clone() Iterator {
	out := NewInt64AllIterator(it.min, it.max)
	out.CopyTagsFrom(it)
	return out
}

// Prints the All iterator as just an "all".
func (it *Int64AllIterator) DebugString(indent int) string {
	return fmt.Sprintf("%s(%s)", strings.Repeat(" ", indent), it.Type())
}

// Next() on an Int64 all iterator is a simple incrementing counter.
// Return the next integer, and mark it as the result.
func (it *Int64AllIterator) Next() (TSVal, bool) {
	NextLogIn(it)
	if it.at == -1 {
		return NextLogOut(it, nil, false)
	}
	val := it.at
	it.at = it.at + 1
	if it.at > it.max {
		it.at = -1
	}
	it.Last = val
	return NextLogOut(it, val, true)
}

// The number of elements in an Int64AllIterator is the size of the range.
// The size is exact.
func (it *Int64AllIterator) Size() (int64, bool) {
	Size := ((it.max - it.min) + 1)
	return Size, true
}

// Check() for an Int64AllIterator is merely seeing if the passed value is
// withing the range, assuming the value is an int64.
func (it *Int64AllIterator) Check(tsv TSVal) bool {
	CheckLogIn(it, tsv)
	v := tsv.(int64)
	if it.min <= v && v <= it.max {
		it.Last = v
		return CheckLogOut(it, v, true)
	}
	return CheckLogOut(it, v, false)
}

// The type of this iterator is an "all". This is important, as it puts it in
// the class of "all iterators.
func (it *Int64AllIterator) Type() string { return "all" }

// There's nothing to optimize about this little iterator.
func (it *Int64AllIterator) Optimize() (Iterator, bool) { return it, false }

// Stats for an Int64AllIterator are simple. Super cheap to do any operation,
// and as big as the range.
func (it *Int64AllIterator) GetStats() *IteratorStats {
	s, _ := it.Size()
	return &IteratorStats{
		CheckCost: 1,
		NextCost:  1,
		Size:      s,
	}
}
