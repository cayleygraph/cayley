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
	"container/list"
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
func (or *OrIterator) Reset() {
	for _, it := range or.internalIterators {
		it.Reset()
	}
	or.currentIterator = -1
}

func (or *OrIterator) Clone() Iterator {
	var newOr *OrIterator
	if or.isShortCircuiting {
		newOr = NewShortCircuitOrIterator()
	} else {
		newOr = NewOrIterator()
	}
	for _, it := range or.internalIterators {
		newOr.AddSubIterator(it.Clone())
	}
	or.CopyTagsFrom(or)
	return newOr
}

// Returns a list.List of the subiterators, in order.
func (or *OrIterator) GetSubIterators() *list.List {
	l := list.New()
	for _, it := range or.internalIterators {
		l.PushBack(it)
	}
	return l
}

// Overrides BaseIterator TagResults, as it needs to add it's own results and
// recurse down it's subiterators.
func (or *OrIterator) TagResults(out *map[string]TSVal) {
	or.BaseIterator.TagResults(out)
	or.internalIterators[or.currentIterator].TagResults(out)
}

// DEPRECATED Returns the ResultTree for this iterator, recurses to it's subiterators.
func (or *OrIterator) GetResultTree() *ResultTree {
	tree := NewResultTree(or.LastResult())
	for _, it := range or.internalIterators {
		tree.AddSubtree(it.GetResultTree())
	}
	return tree
}

// Prints information about this iterator.
func (or *OrIterator) DebugString(indent int) string {
	var total string
	for i, it := range or.internalIterators {
		total += strings.Repeat(" ", indent+2)
		total += fmt.Sprintf("%d:\n%s\n", i, it.DebugString(indent+4))
	}
	var tags string
	for _, k := range or.Tags() {
		tags += fmt.Sprintf("%s;", k)
	}
	spaces := strings.Repeat(" ", indent+2)

	return fmt.Sprintf("%s(%s\n%stags:%s\n%sits:\n%s)",
		strings.Repeat(" ", indent),
		or.Type(),
		spaces,
		tags,
		spaces,
		total)
}

// Add a subiterator to this Or iterator. Order matters.
func (or *OrIterator) AddSubIterator(sub Iterator) {
	or.internalIterators = append(or.internalIterators, sub)
	or.itCount++
}

// Returns the Next value from the Or iterator. Because the Or is the
// union of its subiterators, it must produce from all subiterators -- unless
// it's shortcircuiting, in which case, it's the first one that returns anything.
func (or *OrIterator) Next() (TSVal, bool) {
	NextLogIn(or)
	var curr TSVal
	var exists bool
	firstTime := false
	for {
		if or.currentIterator == -1 {
			or.currentIterator = 0
			firstTime = true
		}
		curIt := or.internalIterators[or.currentIterator]
		curr, exists = curIt.Next()
		if !exists {
			if or.isShortCircuiting && !firstTime {
				return NextLogOut(or, nil, false)
			}
			or.currentIterator++
			if or.currentIterator == or.itCount {
				return NextLogOut(or, nil, false)
			}
		} else {
			or.Last = curr
			return NextLogOut(or, curr, true)
		}
	}
	panic("Somehow broke out of Next() loop in OrIterator")
}

// Checks a value against the iterators, in order.
func (or *OrIterator) checkSubIts(val TSVal) bool {
	var subIsGood = false
	for i, it := range or.internalIterators {
		subIsGood = it.Check(val)
		if subIsGood {
			or.currentIterator = i
			break
		}
	}
	return subIsGood
}

// Check a value against the entire iterator, in order.
func (or *OrIterator) Check(val TSVal) bool {
	CheckLogIn(or, val)
	anyGood := or.checkSubIts(val)
	if !anyGood {
		return CheckLogOut(or, val, false)
	}
	or.Last = val
	return CheckLogOut(or, val, true)
}

// Returns the approximate size of the Or iterator. Because we're dealing
// with a union, we know that the largest we can be is the sum of all the iterators,
// or in the case of short-circuiting, the longest.
func (or *OrIterator) Size() (int64, bool) {
	var val int64
	var b bool
	if or.isShortCircuiting {
		val = 0
		b = true
		for _, it := range or.internalIterators {
			newval, newb := it.Size()
			if val < newval {
				val = newval
			}
			b = newb && b
		}
	} else {
		val = 0
		b = true
		for _, it := range or.internalIterators {
			newval, newb := it.Size()
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
func (or *OrIterator) NextResult() bool {
	if or.currentIterator != -1 {
		return or.internalIterators[or.currentIterator].NextResult()
	}
	return false
}

// Perform or-specific cleanup, of which there currently is none.
func (or *OrIterator) cleanUp() {}

// Close this iterator, and, by extension, close the subiterators.
// Close should be idempotent, and it follows that if it's subiterators
// follow this contract, the And follows the contract.
func (or *OrIterator) Close() {
	or.cleanUp()
	for _, it := range or.internalIterators {
		it.Close()
	}
}

func (or *OrIterator) Optimize() (Iterator, bool) {
	oldItList := or.GetSubIterators()
	itList := optimizeSubIterators(oldItList)
	// Close the replaced iterators (they ought to close themselves, but Close()
	// is idempotent, so this just protects against any machinations).
	closeIteratorList(oldItList, nil)
	newOr := NewOrIterator()
	newOr.isShortCircuiting = or.isShortCircuiting

	// Add the subiterators in order.
	for e := itList.Front(); e != nil; e = e.Next() {
		newOr.AddSubIterator(e.Value.(Iterator))
	}

	// Move the tags hanging on us (like any good replacement).
	newOr.CopyTagsFrom(or)

	// And close ourselves but not our subiterators -- some may still be alive in
	// the new And (they were unchanged upon calling Optimize() on them, at the
	// start).
	or.cleanUp()
	return newOr, true
}

func (or *OrIterator) GetStats() *IteratorStats {
	CheckCost := int64(0)
	NextCost := int64(0)
	Size := int64(0)
	for _, it := range or.internalIterators {
		stats := it.GetStats()
		NextCost += stats.NextCost
		CheckCost += stats.CheckCost
		if or.isShortCircuiting {
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
func (or *OrIterator) Type() string { return "or" }
