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

// Defines the or and short-circuiting or iterator. Or is the union operator for it's subiterators.
// Short-circuiting-or is a little different. It will return values from the first iterator that returns
// values at all, and then stops.
//
// Never reorders the iterators from the order they arrive. It is either the union or the first one.
// May return the same value twice -- once for each branch.

import (
	"fmt"
	"strings"
)

type OrIterator struct {
	BaseIterator
	isShortCircuiting bool
	internalIterators []Iterator
	itCount           int
	currentIterator   int
}

func NewOrIterator() *OrIterator {
	var or OrIterator
	BaseIteratorInit(&or.BaseIterator)
	or.internalIterators = make([]Iterator, 0, 20)
	or.isShortCircuiting = false
	or.currentIterator = -1
	return &or
}

func NewShortCircuitOrIterator() *OrIterator {
	var or OrIterator
	BaseIteratorInit(&or.BaseIterator)
	or.internalIterators = make([]Iterator, 0, 20)
	or.isShortCircuiting = true
	or.currentIterator = -1
	return &or
}

// Reset all internal iterators
func (it *OrIterator) Reset() {
	for _, sub := range it.internalIterators {
		sub.Reset()
	}
	it.currentIterator = -1
}

func (it *OrIterator) Clone() Iterator {
	var or *OrIterator
	if it.isShortCircuiting {
		or = NewShortCircuitOrIterator()
	} else {
		or = NewOrIterator()
	}
	for _, sub := range it.internalIterators {
		or.AddSubIterator(sub.Clone())
	}
	it.CopyTagsFrom(it)
	return or
}

// Returns a list.List of the subiterators, in order. The returned slice must not be modified.
func (it *OrIterator) GetSubIterators() []Iterator {
	return it.internalIterators
}

// Overrides BaseIterator TagResults, as it needs to add it's own results and
// recurse down it's subiterators.
func (it *OrIterator) TagResults(out *map[string]TSVal) {
	it.BaseIterator.TagResults(out)
	it.internalIterators[it.currentIterator].TagResults(out)
}

// DEPRECATED Returns the ResultTree for this iterator, recurses to it's subiterators.
func (it *OrIterator) GetResultTree() *ResultTree {
	tree := NewResultTree(it.LastResult())
	for _, sub := range it.internalIterators {
		tree.AddSubtree(sub.GetResultTree())
	}
	return tree
}

// Prints information about this iterator.
func (it *OrIterator) DebugString(indent int) string {
	var total string
	for i, sub := range it.internalIterators {
		total += strings.Repeat(" ", indent+2)
		total += fmt.Sprintf("%d:\n%s\n", i, sub.DebugString(indent+4))
	}
	var tags string
	for _, k := range it.Tags() {
		tags += fmt.Sprintf("%s;", k)
	}
	spaces := strings.Repeat(" ", indent+2)

	return fmt.Sprintf("%s(%s\n%stags:%s\n%sits:\n%s)",
		strings.Repeat(" ", indent),
		it.Type(),
		spaces,
		tags,
		spaces,
		total)
}

// Add a subiterator to this Or iterator. Order matters.
func (it *OrIterator) AddSubIterator(sub Iterator) {
	it.internalIterators = append(it.internalIterators, sub)
	it.itCount++
}

// Returns the Next value from the Or iterator. Because the Or is the
// union of its subiterators, it must produce from all subiterators -- unless
// it's shortcircuiting, in which case, it's the first one that returns anything.
func (it *OrIterator) Next() (TSVal, bool) {
	NextLogIn(it)
	var curr TSVal
	var exists bool
	firstTime := false
	for {
		if it.currentIterator == -1 {
			it.currentIterator = 0
			firstTime = true
		}
		curIt := it.internalIterators[it.currentIterator]
		curr, exists = curIt.Next()
		if !exists {
			if it.isShortCircuiting && !firstTime {
				return NextLogOut(it, nil, false)
			}
			it.currentIterator++
			if it.currentIterator == it.itCount {
				return NextLogOut(it, nil, false)
			}
		} else {
			it.Last = curr
			return NextLogOut(it, curr, true)
		}
	}
	panic("Somehow broke out of Next() loop in OrIterator")
}

// Checks a value against the iterators, in order.
func (it *OrIterator) checkSubIts(val TSVal) bool {
	var subIsGood = false
	for i, sub := range it.internalIterators {
		subIsGood = sub.Check(val)
		if subIsGood {
			it.currentIterator = i
			break
		}
	}
	return subIsGood
}

// Check a value against the entire iterator, in order.
func (it *OrIterator) Check(val TSVal) bool {
	CheckLogIn(it, val)
	anyGood := it.checkSubIts(val)
	if !anyGood {
		return CheckLogOut(it, val, false)
	}
	it.Last = val
	return CheckLogOut(it, val, true)
}

// Returns the approximate size of the Or iterator. Because we're dealing
// with a union, we know that the largest we can be is the sum of all the iterators,
// or in the case of short-circuiting, the longest.
func (it *OrIterator) Size() (int64, bool) {
	var val int64
	var b bool
	if it.isShortCircuiting {
		val = 0
		b = true
		for _, sub := range it.internalIterators {
			newval, newb := sub.Size()
			if val < newval {
				val = newval
			}
			b = newb && b
		}
	} else {
		val = 0
		b = true
		for _, sub := range it.internalIterators {
			newval, newb := sub.Size()
			val += newval
			b = newb && b
		}
	}
	return val, b
}

// An Or has no NextResult of its own -- that is, there are no other values
// which satisfy our previous result that are not the result itself. Our
// subiterators might, however, so just pass the call recursively. In the case of
// shortcircuiting, only allow new results from the currently checked iterator
func (it *OrIterator) NextResult() bool {
	if it.currentIterator != -1 {
		return it.internalIterators[it.currentIterator].NextResult()
	}
	return false
}

// Perform or-specific cleanup, of which there currently is none.
func (it *OrIterator) cleanUp() {}

// Close this iterator, and, by extension, close the subiterators.
// Close should be idempotent, and it follows that if it's subiterators
// follow this contract, the And follows the contract.
func (it *OrIterator) Close() {
	it.cleanUp()
	for _, sub := range it.internalIterators {
		sub.Close()
	}
}

func (it *OrIterator) Optimize() (Iterator, bool) {
	old := it.GetSubIterators()
	optIts := optimizeSubIterators(old)
	// Close the replaced iterators (they ought to close themselves, but Close()
	// is idempotent, so this just protects against any machinations).
	closeIteratorList(old, nil)
	newOr := NewOrIterator()
	newOr.isShortCircuiting = it.isShortCircuiting

	// Add the subiterators in order.
	for _, o := range optIts {
		newOr.AddSubIterator(o)
	}

	// Move the tags hanging on us (like any good replacement).
	newOr.CopyTagsFrom(it)

	// And close ourselves but not our subiterators -- some may still be alive in
	// the new And (they were unchanged upon calling Optimize() on them, at the
	// start).
	it.cleanUp()
	return newOr, true
}

func (it *OrIterator) GetStats() *IteratorStats {
	CheckCost := int64(0)
	NextCost := int64(0)
	Size := int64(0)
	for _, sub := range it.internalIterators {
		stats := sub.GetStats()
		NextCost += stats.NextCost
		CheckCost += stats.CheckCost
		if it.isShortCircuiting {
			if Size < stats.Size {
				Size = stats.Size
			}
		} else {
			Size += stats.Size
		}
	}
	return &IteratorStats{
		CheckCost: CheckCost,
		NextCost:  NextCost,
		Size:      Size,
	}

}

// Register this as an "or" iterator.
func (it *OrIterator) Type() string { return "or" }
