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

// Defines one of the base iterators, the Fixed iterator. A fixed iterator is quite simple; it
// contains an explicit fixed array of values.
//
// A fixed iterator requires an Equality function to be passed to it, by reason that TSVal, the
// opaque Triple store value, may not answer to ==.

import (
	"fmt"
	"strings"
)

// A Fixed iterator consists of it's values, an index (where it is in the process of Next()ing) and
// an equality function.
type FixedIterator struct {
	BaseIterator
	values    []TSVal
	lastIndex int
	cmp       Equality
}

// Define the signature of an equality function.
type Equality func(a, b TSVal) bool

// Define an equality function of purely ==, which works for native types.
func BasicEquality(a, b TSVal) bool {
	if a == b {
		return true
	}
	return false
}

// Creates a new Fixed iterator based around == equality.
func newFixedIterator() *FixedIterator {
	return NewFixedIteratorWithCompare(BasicEquality)
}

// Creates a new Fixed iterator with a custom comparitor.
func NewFixedIteratorWithCompare(compareFn Equality) *FixedIterator {
	var it FixedIterator
	BaseIteratorInit(&it.BaseIterator)
	it.values = make([]TSVal, 0, 20)
	it.lastIndex = 0
	it.cmp = compareFn
	return &it
}

func (f *FixedIterator) Reset() {
	f.lastIndex = 0
}

func (f *FixedIterator) Close() {
}

func (f *FixedIterator) Clone() Iterator {
	out := NewFixedIteratorWithCompare(f.cmp)
	for _, val := range f.values {
		out.AddValue(val)
	}
	out.CopyTagsFrom(f)
	return out
}

// Add a value to the iterator. The array now contains this value.
// TODO(barakmich): This ought to be a set someday, disallowing repeated values.
func (f *FixedIterator) AddValue(v TSVal) {
	f.values = append(f.values, v)
}

// Print some information about the iterator.
func (f *FixedIterator) DebugString(indent int) string {
	value := ""
	if len(f.values) > 0 {
		value = fmt.Sprint(f.values[0])
	}
	return fmt.Sprintf("%s(%s tags: %s Size: %d id0: %d)",
		strings.Repeat(" ", indent),
		f.Type(),
		f.FixedTags(),
		len(f.values),
		value,
	)
}

// Register this iterator as a Fixed iterator.
func (f *FixedIterator) Type() string {
	return "fixed"
}

// Check if the passed value is equal to one of the values stored in the iterator.
func (f *FixedIterator) Check(v TSVal) bool {
	// Could be optimized by keeping it sorted or using a better datastructure.
	// However, for fixed iterators, which are by definition kind of tiny, this
	// isn't a big issue.
	CheckLogIn(f, v)
	for _, x := range f.values {
		if f.cmp(x, v) {
			f.Last = x
			return CheckLogOut(f, v, true)
		}
	}
	return CheckLogOut(f, v, false)
}

// Return the next stored value from the iterator.
func (f *FixedIterator) Next() (TSVal, bool) {
	NextLogIn(f)
	if f.lastIndex == len(f.values) {
		return NextLogOut(f, nil, false)
	}
	out := f.values[f.lastIndex]
	f.Last = out
	f.lastIndex++
	return NextLogOut(f, out, true)
}

// Optimize() for a Fixed iterator is simple. Returns a Null iterator if it's empty
// (so that other iterators upstream can treat this as null) or there is no
// optimization.
func (f *FixedIterator) Optimize() (Iterator, bool) {

	if len(f.values) == 1 && f.values[0] == nil {
		return &NullIterator{}, true
	}

	return f, false
}

// Size is the number of values stored.
func (f *FixedIterator) Size() (int64, bool) {
	return int64(len(f.values)), true
}

// As we right now have to scan the entire list, Next and Check are linear with the
// size. However, a better data structure could remove these limits.
func (a *FixedIterator) GetStats() *IteratorStats {
	return &IteratorStats{
		CheckCost: int64(len(a.values)),
		NextCost:  int64(len(a.values)),
		Size:      int64(len(a.values)),
	}
}
