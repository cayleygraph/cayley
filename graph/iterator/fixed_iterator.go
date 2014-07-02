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
// A fixed iterator requires an Equality function to be passed to it, by reason that graph.TSVal, the
// opaque Triple store value, may not answer to ==.

import (
	"fmt"
	"strings"

	"github.com/google/cayley/graph"
)

// A Fixed iterator consists of it's values, an index (where it is in the process of Next()ing) and
// an equality function.
type Fixed struct {
	Base
	values    []graph.TSVal
	lastIndex int
	cmp       Equality
}

// Define the signature of an equality function.
type Equality func(a, b graph.TSVal) bool

// Define an equality function of purely ==, which works for native types.
func BasicEquality(a, b graph.TSVal) bool {
	if a == b {
		return true
	}
	return false
}

// Creates a new Fixed iterator based around == equality.
func newFixed() *Fixed {
	return NewFixedIteratorWithCompare(BasicEquality)
}

// Creates a new Fixed iterator with a custom comparitor.
func NewFixedIteratorWithCompare(compareFn Equality) *Fixed {
	var it Fixed
	BaseInit(&it.Base)
	it.values = make([]graph.TSVal, 0, 20)
	it.lastIndex = 0
	it.cmp = compareFn
	return &it
}

func (it *Fixed) Reset() {
	it.lastIndex = 0
}

func (it *Fixed) Close() {}

func (it *Fixed) Clone() graph.Iterator {
	out := NewFixedIteratorWithCompare(it.cmp)
	for _, val := range it.values {
		out.AddValue(val)
	}
	out.CopyTagsFrom(it)
	return out
}

// Add a value to the iterator. The array now contains this value.
// TODO(barakmich): This ought to be a set someday, disallowing repeated values.
func (it *Fixed) AddValue(v graph.TSVal) {
	it.values = append(it.values, v)
}

// Print some information about the iterator.
func (it *Fixed) DebugString(indent int) string {
	value := ""
	if len(it.values) > 0 {
		value = fmt.Sprint(it.values[0])
	}
	return fmt.Sprintf("%s(%s tags: %s Size: %d id0: %d)",
		strings.Repeat(" ", indent),
		it.Type(),
		it.FixedTags(),
		len(it.values),
		value,
	)
}

// Register this iterator as a Fixed iterator.
func (it *Fixed) Type() string {
	return "fixed"
}

// Check if the passed value is equal to one of the values stored in the iterator.
func (it *Fixed) Check(v graph.TSVal) bool {
	// Could be optimized by keeping it sorted or using a better datastructure.
	// However, for fixed iterators, which are by definition kind of tiny, this
	// isn't a big issue.
	graph.CheckLogIn(it, v)
	for _, x := range it.values {
		if it.cmp(x, v) {
			it.Last = x
			return graph.CheckLogOut(it, v, true)
		}
	}
	return graph.CheckLogOut(it, v, false)
}

// Return the next stored value from the iterator.
func (it *Fixed) Next() (graph.TSVal, bool) {
	graph.NextLogIn(it)
	if it.lastIndex == len(it.values) {
		return graph.NextLogOut(it, nil, false)
	}
	out := it.values[it.lastIndex]
	it.Last = out
	it.lastIndex++
	return graph.NextLogOut(it, out, true)
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

// As we right now have to scan the entire list, Next and Check are linear with the
// size. However, a better data structure could remove these limits.
func (it *Fixed) Stats() graph.IteratorStats {
	return graph.IteratorStats{
		CheckCost: int64(len(it.values)),
		NextCost:  int64(len(it.values)),
		Size:      int64(len(it.values)),
	}
}
