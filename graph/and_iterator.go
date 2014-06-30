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
	checkList         []Iterator
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
func (it *AndIterator) Reset() {
	it.primaryIt.Reset()
	for _, sub := range it.internalIterators {
		sub.Reset()
	}
	it.checkList = nil
}

func (it *AndIterator) Clone() Iterator {
	and := NewAndIterator()
	and.AddSubIterator(it.primaryIt.Clone())
	and.CopyTagsFrom(it)
	for _, sub := range it.internalIterators {
		and.AddSubIterator(sub.Clone())
	}
	if it.checkList != nil {
		and.optimizeCheck()
	}
	return and
}

// Returns a slice of the subiterators, in order (primary iterator first).
func (it *AndIterator) GetSubIterators() []Iterator {
	iters := make([]Iterator, len(it.internalIterators)+1)
	iters[0] = it.primaryIt
	copy(iters[1:], it.internalIterators)
	return iters
}

// Overrides BaseIterator TagResults, as it needs to add it's own results and
// recurse down it's subiterators.
func (it *AndIterator) TagResults(out *map[string]TSVal) {
	it.BaseIterator.TagResults(out)
	if it.primaryIt != nil {
		it.primaryIt.TagResults(out)
	}
	for _, sub := range it.internalIterators {
		sub.TagResults(out)
	}
}

// DEPRECATED Returns the ResultTree for this iterator, recurses to it's subiterators.
func (it *AndIterator) GetResultTree() *ResultTree {
	tree := NewResultTree(it.LastResult())
	tree.AddSubtree(it.primaryIt.GetResultTree())
	for _, sub := range it.internalIterators {
		tree.AddSubtree(sub.GetResultTree())
	}
	return tree
}

// Prints information about this iterator.
func (it *AndIterator) DebugString(indent int) string {
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

	return fmt.Sprintf("%s(%s %d\n%stags:%s\n%sprimary_it:\n%s\n%sother_its:\n%s)",
		strings.Repeat(" ", indent),
		it.Type(),
		it.GetUid(),
		spaces,
		tags,
		spaces,
		it.primaryIt.DebugString(indent+4),
		spaces,
		total)
}

// Add a subiterator to this And iterator.
//
// The first iterator that is added becomes the primary iterator. This is
// important. Calling Optimize() is the way to change the order based on
// subiterator statistics. Without Optimize(), the order added is the order
// used.
func (it *AndIterator) AddSubIterator(sub Iterator) {
	if it.itCount > 0 {
		it.internalIterators = append(it.internalIterators, sub)
		it.itCount++
		return
	}
	it.primaryIt = sub
	it.itCount++
}

// Returns the Next value from the And iterator. Because the And is the
// intersection of its subiterators, it must choose one subiterator to produce a
// candidate, and check this value against the subiterators. A productive choice
// of primary iterator is therefore very important.
func (it *AndIterator) Next() (TSVal, bool) {
	NextLogIn(it)
	var curr TSVal
	var exists bool
	for {
		curr, exists = it.primaryIt.Next()
		if !exists {
			return NextLogOut(it, nil, false)
		}
		if it.checkSubIts(curr) {
			it.Last = curr
			return NextLogOut(it, curr, true)
		}
	}
	panic("Somehow broke out of Next() loop in AndIterator")
}

// Checks a value against the non-primary iterators, in order.
func (it *AndIterator) checkSubIts(val TSVal) bool {
	var subIsGood = true
	for _, sub := range it.internalIterators {
		subIsGood = sub.Check(val)
		if !subIsGood {
			break
		}
	}
	return subIsGood
}

func (it *AndIterator) checkCheckList(val TSVal) bool {
	ok := true
	for _, c := range it.checkList {
		ok = c.Check(val)
		if !ok {
			break
		}
	}
	if ok {
		it.Last = val
	}
	return CheckLogOut(it, val, ok)
}

// Check a value against the entire iterator, in order.
func (it *AndIterator) Check(val TSVal) bool {
	CheckLogIn(it, val)
	if it.checkList != nil {
		return it.checkCheckList(val)
	}
	mainGood := it.primaryIt.Check(val)
	if !mainGood {
		return CheckLogOut(it, val, false)
	}
	othersGood := it.checkSubIts(val)
	if !othersGood {
		return CheckLogOut(it, val, false)
	}
	it.Last = val
	return CheckLogOut(it, val, true)
}

// Returns the approximate size of the And iterator. Because we're dealing
// with an intersection, we know that the largest we can be is the size of the
// smallest iterator. This is the heuristic we shall follow. Better heuristics
// welcome.
func (it *AndIterator) Size() (int64, bool) {
	val, b := it.primaryIt.Size()
	for _, sub := range it.internalIterators {
		newval, newb := sub.Size()
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
func (it *AndIterator) NextResult() bool {
	if it.primaryIt.NextResult() {
		return true
	}
	for _, sub := range it.internalIterators {
		if sub.NextResult() {
			return true
		}
	}
	return false
}

// Perform and-specific cleanup, of which there currently is none.
func (it *AndIterator) cleanUp() {}

// Close this iterator, and, by extension, close the subiterators.
// Close should be idempotent, and it follows that if it's subiterators
// follow this contract, the And follows the contract.
func (it *AndIterator) Close() {
	it.cleanUp()
	it.primaryIt.Close()
	for _, sub := range it.internalIterators {
		sub.Close()
	}
}

// Register this as an "and" iterator.
func (it *AndIterator) Type() string { return "and" }
