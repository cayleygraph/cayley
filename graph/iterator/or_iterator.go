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

// Defines the or and short-circuiting or iterator. Or is the union operator for it's subiterators.
// Short-circuiting-or is a little different. It will return values from the first graph.iterator that returns
// values at all, and then stops.
//
// Never reorders the iterators from the order they arrive. It is either the union or the first one.
// May return the same value twice -- once for each branch.

import (
	"fmt"
	"strings"

	"github.com/google/cayley/graph"
)

type Or struct {
	Base
	uid               uint64
	tags              graph.Tagger
	isShortCircuiting bool
	internalIterators []graph.Iterator
	itCount           int
	currentIterator   int
	result            graph.Value
}

func NewOr() *Or {
	return &Or{
		uid:               NextUID(),
		internalIterators: make([]graph.Iterator, 0, 20),
		currentIterator:   -1,
	}
}

func NewShortCircuitOr() *Or {
	return &Or{
		uid:               NextUID(),
		internalIterators: make([]graph.Iterator, 0, 20),
		isShortCircuiting: true,
		currentIterator:   -1,
	}
}

func (it *Or) UID() uint64 {
	return it.uid
}

// Reset all internal iterators
func (it *Or) Reset() {
	for _, sub := range it.internalIterators {
		sub.Reset()
	}
	it.currentIterator = -1
}

func (it *Or) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *Or) Clone() graph.Iterator {
	var or *Or
	if it.isShortCircuiting {
		or = NewShortCircuitOr()
	} else {
		or = NewOr()
	}
	for _, sub := range it.internalIterators {
		or.AddSubIterator(sub.Clone())
	}
	or.tags.CopyFrom(it)
	return or
}

// Returns a list.List of the subiterators, in order. The returned slice must not be modified.
func (it *Or) SubIterators() []graph.Iterator {
	return it.internalIterators
}

// Overrides BaseIterator TagResults, as it needs to add it's own results and
// recurse down it's subiterators.
func (it *Or) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}

	it.internalIterators[it.currentIterator].TagResults(dst)
}

// DEPRECATED Returns the ResultTree for this graph.iterator, recurses to it's subiterators.
func (it *Or) ResultTree() *graph.ResultTree {
	tree := graph.NewResultTree(it.Result())
	for _, sub := range it.internalIterators {
		tree.AddSubtree(sub.ResultTree())
	}
	return tree
}

// Prints information about this graph.iterator.
func (it *Or) DebugString(indent int) string {
	var total string
	for i, sub := range it.internalIterators {
		total += strings.Repeat(" ", indent+2)
		total += fmt.Sprintf("%d:\n%s\n", i, sub.DebugString(indent+4))
	}
	var tags string
	for _, k := range it.tags.Tags() {
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

// Add a subiterator to this Or graph.iterator. Order matters.
func (it *Or) AddSubIterator(sub graph.Iterator) {
	it.internalIterators = append(it.internalIterators, sub)
	it.itCount++
}

// Returns the Next value from the Or graph.iterator. Because the Or is the
// union of its subiterators, it must produce from all subiterators -- unless
// it's shortcircuiting, in which case, it's the first one that returns anything.
func (it *Or) Next() (graph.Value, bool) {
	graph.NextLogIn(it)
	var curr graph.Value
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
				return graph.NextLogOut(it, nil, false)
			}
			it.currentIterator++
			if it.currentIterator == it.itCount {
				return graph.NextLogOut(it, nil, false)
			}
		} else {
			it.result = curr
			return graph.NextLogOut(it, curr, true)
		}
	}
	panic("unreachable")
}

func (it *Or) Result() graph.Value {
	return it.result
}

// Checks a value against the iterators, in order.
func (it *Or) checkSubIts(val graph.Value) bool {
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

// Check a value against the entire graph.iterator, in order.
func (it *Or) Check(val graph.Value) bool {
	graph.CheckLogIn(it, val)
	anyGood := it.checkSubIts(val)
	if !anyGood {
		return graph.CheckLogOut(it, val, false)
	}
	it.result = val
	return graph.CheckLogOut(it, val, true)
}

// Returns the approximate size of the Or graph.iterator. Because we're dealing
// with a union, we know that the largest we can be is the sum of all the iterators,
// or in the case of short-circuiting, the longest.
func (it *Or) Size() (int64, bool) {
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
// shortcircuiting, only allow new results from the currently checked graph.iterator
func (it *Or) NextResult() bool {
	if it.currentIterator != -1 {
		return it.internalIterators[it.currentIterator].NextResult()
	}
	return false
}

// Perform or-specific cleanup, of which there currently is none.
func (it *Or) cleanUp() {}

// Close this graph.iterator, and, by extension, close the subiterators.
// Close should be idempotent, and it follows that if it's subiterators
// follow this contract, the And follows the contract.
func (it *Or) Close() {
	it.cleanUp()
	for _, sub := range it.internalIterators {
		sub.Close()
	}
}

func (it *Or) Optimize() (graph.Iterator, bool) {
	old := it.SubIterators()
	optIts := optimizeSubIterators(old)
	// Close the replaced iterators (they ought to close themselves, but Close()
	// is idempotent, so this just protects against any machinations).
	closeIteratorList(old, nil)
	newOr := NewOr()
	newOr.isShortCircuiting = it.isShortCircuiting

	// Add the subiterators in order.
	for _, o := range optIts {
		newOr.AddSubIterator(o)
	}

	// Move the tags hanging on us (like any good replacement).
	newOr.tags.CopyFrom(it)

	// And close ourselves but not our subiterators -- some may still be alive in
	// the new And (they were unchanged upon calling Optimize() on them, at the
	// start).
	it.cleanUp()
	return newOr, true
}

func (it *Or) Stats() graph.IteratorStats {
	CheckCost := int64(0)
	NextCost := int64(0)
	Size := int64(0)
	for _, sub := range it.internalIterators {
		stats := sub.Stats()
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
	return graph.IteratorStats{
		CheckCost: CheckCost,
		NextCost:  NextCost,
		Size:      Size,
	}

}

// Register this as an "or" graph.iterator.
func (it *Or) Type() graph.Type { return graph.Or }
