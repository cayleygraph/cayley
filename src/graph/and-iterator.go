// Defines the And iterator, one of the base iterators. And requires no
// knowledge of the constituent TripleStore; its sole purpose is to act as an
// intersection operator across the subiterators it is given. If one iterator
// contains [1,3,5] and another [2,3,4] -- then And is an iterator that
// 'contains' [3]
//
// It accomplishes this in one of two ways. If it is a Next()ed iterator (that
// is, it is a top level iterator, or on the "Next() path", then it will Next()
// it's primary iterator (helpfully, and.primary_it) and Check() the resultant
// value against it's other iterators. If it matches all of them, then it
// returns that value. Otherwise, it repeats the process.
//
// If it's on a Check() path, it merely Check()s every iterator, and returns the
// logical AND of each result.

package graph

import (
	"container/list"
	"fmt"
	"strings"
)

// The And iterator. Consists of a BaseIterator and a number of subiterators, the primary of which will
// be Next()ed if next is called.
type AndIterator struct {
	BaseIterator
	internalIterators []Iterator
	itCount           int
	primaryIt         Iterator
	checkList         *list.List
}

// Creates a new And iterator.
func NewAndIterator() *AndIterator {
	var and AndIterator
	BaseIteratorInit(&and.BaseIterator)
	and.internalIterators = make([]Iterator, 0, 20)
	and.checkList = nil
	return &and
}

// Reset all internal iterators
func (and *AndIterator) Reset() {
	and.primaryIt.Reset()
	for _, it := range and.internalIterators {
		it.Reset()
	}
	and.checkList = nil
}

func (and *AndIterator) Clone() Iterator {
	newAnd := NewAndIterator()
	newAnd.AddSubIterator(and.primaryIt.Clone())
	newAnd.CopyTagsFrom(and)
	for _, it := range and.internalIterators {
		newAnd.AddSubIterator(it.Clone())
	}
	if and.checkList != nil {
		newAnd.optimizeCheck()
	}
	return newAnd
}

// Returns a list.List of the subiterators, in order (primary iterator first).
func (and *AndIterator) GetSubIterators() *list.List {
	l := list.New()
	l.PushBack(and.primaryIt)
	for _, it := range and.internalIterators {
		l.PushBack(it)
	}
	return l
}

// Overrides BaseIterator TagResults, as it needs to add it's own results and
// recurse down it's subiterators.
func (and *AndIterator) TagResults(out *map[string]TSVal) {
	and.BaseIterator.TagResults(out)
	if and.primaryIt != nil {
		and.primaryIt.TagResults(out)
	}
	for _, it := range and.internalIterators {
		it.TagResults(out)
	}
}

// DEPRECATED Returns the ResultTree for this iterator, recurses to it's subiterators.
func (and *AndIterator) GetResultTree() *ResultTree {
	tree := NewResultTree(and.LastResult())
	tree.AddSubtree(and.primaryIt.GetResultTree())
	for _, it := range and.internalIterators {
		tree.AddSubtree(it.GetResultTree())
	}
	return tree
}

// Prints information about this iterator.
func (and *AndIterator) DebugString(indent int) string {
	var total string
	for i, it := range and.internalIterators {
		total += strings.Repeat(" ", indent+2)
		total += fmt.Sprintf("%d:\n%s\n", i, it.DebugString(indent+4))
	}
	var tags string
	for _, k := range and.Tags() {
		tags += fmt.Sprintf("%s;", k)
	}
	spaces := strings.Repeat(" ", indent+2)

	return fmt.Sprintf("%s(%s %d\n%stags:%s\n%sprimary_it:\n%s\n%sother_its:\n%s)",
		strings.Repeat(" ", indent),
		and.Type(),
		and.GetUid(),
		spaces,
		tags,
		spaces,
		and.primaryIt.DebugString(indent+4),
		spaces,
		total)
}

// Add a subiterator to this And iterator.
//
// The first iterator that is added becomes the primary iterator. This is
// important. Calling Optimize() is the way to change the order based on
// subiterator statistics. Without Optimize(), the order added is the order
// used.
func (and *AndIterator) AddSubIterator(sub Iterator) {
	if and.itCount > 0 {
		and.internalIterators = append(and.internalIterators, sub)
		and.itCount++
		return
	}
	and.primaryIt = sub
	and.itCount++
}

// Returns the Next value from the And iterator. Because the And is the
// intersection of its subiterators, it must choose one subiterator to produce a
// candidate, and check this value against the subiterators. A productive choice
// of primary iterator is therefore very important.
func (and *AndIterator) Next() (TSVal, bool) {
	NextLogIn(and)
	var curr TSVal
	var exists bool
	for {

		curr, exists = and.primaryIt.Next()
		if !exists {
			return NextLogOut(and, nil, false)
		}
		if and.checkSubIts(curr) {
			and.Last = curr
			return NextLogOut(and, curr, true)
		}
	}
	panic("Somehow broke out of Next() loop in AndIterator")
}

// Checks a value against the non-primary iterators, in order.
func (and *AndIterator) checkSubIts(val TSVal) bool {
	var subIsGood = true
	for _, it := range and.internalIterators {
		subIsGood = it.Check(val)
		if !subIsGood {
			break
		}
	}
	return subIsGood
}

func (and *AndIterator) checkCheckList(val TSVal) bool {
	var isGood = true
	for e := and.checkList.Front(); e != nil; e = e.Next() {
		isGood = e.Value.(Iterator).Check(val)
		if !isGood {
			break
		}
	}
	return CheckLogOut(and, val, isGood)
}

// Check a value against the entire iterator, in order.
func (and *AndIterator) Check(val TSVal) bool {
	CheckLogIn(and, val)
	if and.checkList != nil {
		return and.checkCheckList(val)
	}
	mainGood := and.primaryIt.Check(val)
	if !mainGood {
		return CheckLogOut(and, val, false)
	}
	othersGood := and.checkSubIts(val)
	if !othersGood {
		return CheckLogOut(and, val, false)
	}
	and.Last = val
	return CheckLogOut(and, val, true)
}

// Returns the approximate size of the And iterator. Because we're dealing
// with an intersection, we know that the largest we can be is the size of the
// smallest iterator. This is the heuristic we shall follow. Better heuristics
// welcome.
func (and *AndIterator) Size() (int64, bool) {
	val, b := and.primaryIt.Size()
	for _, it := range and.internalIterators {
		newval, newb := it.Size()
		if val > newval {
			val = newval
		}
		b = newb && b
	}
	return val, b
}

// An And has no NextResult of its own -- that is, there are no other values
// which satisfy our previous result that are not the result itself. Our
// subiterators might, however, so just pass the call recursively.
func (and *AndIterator) NextResult() bool {
	if and.primaryIt.NextResult() {
		return true
	}
	for _, it := range and.internalIterators {
		if it.NextResult() {
			return true
		}
	}
	return false
}

// Perform and-specific cleanup, of which there currently is none.
func (and *AndIterator) cleanUp() {
}

// Close this iterator, and, by extension, close the subiterators.
// Close should be idempotent, and it follows that if it's subiterators
// follow this contract, the And follows the contract.
func (and *AndIterator) Close() {
	and.cleanUp()
	and.primaryIt.Close()
	for _, it := range and.internalIterators {
		it.Close()
	}
}

// Register this as an "and" iterator.
func (and *AndIterator) Type() string { return "and" }
