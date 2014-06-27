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
func (a *Int64AllIterator) Reset() {
	a.at = a.min
}

func (a *Int64AllIterator) Close() {
}

func (a *Int64AllIterator) Clone() Iterator {
	out := NewInt64AllIterator(a.min, a.max)
	out.CopyTagsFrom(a)
	return out
}

// Prints the All iterator as just an "all".
func (a *Int64AllIterator) DebugString(indent int) string {
	return fmt.Sprintf("%s(%s)", strings.Repeat(" ", indent), a.Type())
}

// Next() on an Int64 all iterator is a simple incrementing counter.
// Return the next integer, and mark it as the result.
func (a *Int64AllIterator) Next() (TSVal, bool) {
	NextLogIn(a)
	if a.at == -1 {
		return NextLogOut(a, nil, false)
	}
	val := a.at
	a.at = a.at + 1
	if a.at > a.max {
		a.at = -1
	}
	a.Last = val
	return NextLogOut(a, val, true)
}

// The number of elements in an Int64AllIterator is the size of the range.
// The size is exact.
func (a *Int64AllIterator) Size() (int64, bool) {
	Size := ((a.max - a.min) + 1)
	return Size, true
}

// Check() for an Int64AllIterator is merely seeing if the passed value is
// withing the range, assuming the value is an int64.
func (a *Int64AllIterator) Check(tsv TSVal) bool {
	CheckLogIn(a, tsv)
	v := tsv.(int64)
	if a.min <= v && v <= a.max {
		a.Last = v
		return CheckLogOut(a, v, true)
	}
	return CheckLogOut(a, v, false)
}

// The type of this iterator is an "all". This is important, as it puts it in
// the class of "all iterators.
func (a *Int64AllIterator) Type() string { return "all" }

// There's nothing to optimize about this little iterator.
func (a *Int64AllIterator) Optimize() (Iterator, bool) { return a, false }

// Stats for an Int64AllIterator are simple. Super cheap to do any operation,
// and as big as the range.
func (a *Int64AllIterator) GetStats() *IteratorStats {
	s, _ := a.Size()
	return &IteratorStats{
		CheckCost: 1,
		NextCost:  1,
		Size:      s,
	}
}
