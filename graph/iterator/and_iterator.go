// Defines the And iterator, one of the base iterators. And requires no
// knowledge of the constituent QuadStore; its sole purpose is to act as an
// intersection operator across the subiterators it is given. If one iterator
// contains [1,3,5] and another [2,3,4] -- then And is an iterator that
// 'contains' [3]
//
// It accomplishes this in one of two ways. If it is a Next()ed iterator (that
// is, it is a top level iterator, or on the "Next() path", then it will Next()
// it's primary iterator (helpfully, and.primary_it) and Contains() the resultant
// value against it's other iterators. If it matches all of them, then it
// returns that value. Otherwise, it repeats the process.
//
// If it's on a Contains() path, it merely Contains()s every iterator, and returns the
// logical AND of each result.

package iterator

import (
	"github.com/google/cayley/graph"
)

// The And iterator. Consists of a number of subiterators, the primary of which will
// be Next()ed if next is called.
type And struct {
	uid               uint64
	tags              graph.Tagger
	internalIterators []graph.Iterator
	itCount           int
	primaryIt         graph.Iterator
	checkList         []graph.Iterator
	result            graph.Value
	runstats          graph.IteratorStats
}

// Creates a new And iterator.
func NewAnd() *And {
	return &And{
		uid:               NextUID(),
		internalIterators: make([]graph.Iterator, 0, 20),
	}
}

func (it *And) UID() uint64 {
	return it.uid
}

// Reset all internal iterators
func (it *And) Reset() {
	it.result = nil
	it.primaryIt.Reset()
	for _, sub := range it.internalIterators {
		sub.Reset()
	}
	it.checkList = nil
}

func (it *And) Tagger() *graph.Tagger {
	return &it.tags
}

// An extended TagResults, as it needs to add it's own results and
// recurse down it's subiterators.
func (it *And) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}

	if it.primaryIt != nil {
		it.primaryIt.TagResults(dst)
	}
	for _, sub := range it.internalIterators {
		sub.TagResults(dst)
	}
}

func (it *And) Clone() graph.Iterator {
	and := NewAnd()
	and.AddSubIterator(it.primaryIt.Clone())
	and.tags.CopyFrom(it)
	for _, sub := range it.internalIterators {
		and.AddSubIterator(sub.Clone())
	}
	if it.checkList != nil {
		and.optimizeContains()
	}
	return and
}

// Returns a slice of the subiterators, in order (primary iterator first).
func (it *And) SubIterators() []graph.Iterator {
	iters := make([]graph.Iterator, len(it.internalIterators)+1)
	iters[0] = it.primaryIt
	copy(iters[1:], it.internalIterators)
	return iters
}

// DEPRECATED Returns the ResultTree for this iterator, recurses to it's subiterators.
func (it *And) ResultTree() *graph.ResultTree {
	tree := graph.NewResultTree(it.Result())
	tree.AddSubtree(it.primaryIt.ResultTree())
	for _, sub := range it.internalIterators {
		tree.AddSubtree(sub.ResultTree())
	}
	return tree
}

func (it *And) Describe() graph.Description {
	subIts := make([]graph.Description, len(it.internalIterators))
	for i, sub := range it.internalIterators {
		subIts[i] = sub.Describe()
	}
	primary := it.primaryIt.Describe()
	return graph.Description{
		UID:       it.UID(),
		Type:      it.Type(),
		Tags:      it.tags.Tags(),
		Iterator:  &primary,
		Iterators: subIts,
	}
}

// Add a subiterator to this And iterator.
//
// The first iterator that is added becomes the primary iterator. This is
// important. Calling Optimize() is the way to change the order based on
// subiterator statistics. Without Optimize(), the order added is the order
// used.
func (it *And) AddSubIterator(sub graph.Iterator) {
	if it.itCount > 0 {
		it.internalIterators = append(it.internalIterators, sub)
		it.itCount++
		return
	}
	it.primaryIt = sub
	it.itCount++
}

// Returns advances the And iterator. Because the And is the intersection of its
// subiterators, it must choose one subiterator to produce a candidate, and check
// this value against the subiterators. A productive choice of primary iterator
// is therefore very important.
func (it *And) Next() bool {
	graph.NextLogIn(it)
	it.runstats.Next += 1
	for graph.Next(it.primaryIt) {
		curr := it.primaryIt.Result()
		if it.subItsContain(curr, nil) {
			it.result = curr
			return graph.NextLogOut(it, curr, true)
		}
	}
	return graph.NextLogOut(it, nil, false)
}

func (it *And) Result() graph.Value {
	return it.result
}

// Checks a value against the non-primary iterators, in order.
func (it *And) subItsContain(val graph.Value, lastResult graph.Value) bool {
	var subIsGood = true
	for i, sub := range it.internalIterators {
		subIsGood = sub.Contains(val)
		if !subIsGood {
			if lastResult != nil {
				for j := 0; j < i; j++ {
					it.internalIterators[j].Contains(lastResult)
				}
			}
			break
		}
	}
	return subIsGood
}

func (it *And) checkContainsList(val graph.Value, lastResult graph.Value) bool {
	ok := true
	for i, c := range it.checkList {
		ok = c.Contains(val)
		if !ok {
			if lastResult != nil {
				for j := 0; j < i; j++ {
					it.checkList[j].Contains(lastResult)
				}
			}
			break
		}
	}
	if ok {
		it.result = val
	}
	return graph.ContainsLogOut(it, val, ok)
}

// Check a value against the entire iterator, in order.
func (it *And) Contains(val graph.Value) bool {
	graph.ContainsLogIn(it, val)
	it.runstats.Contains += 1
	lastResult := it.result
	if it.checkList != nil {
		return it.checkContainsList(val, lastResult)
	}
	mainGood := it.primaryIt.Contains(val)
	if mainGood {
		othersGood := it.subItsContain(val, lastResult)
		if othersGood {
			it.result = val
			return graph.ContainsLogOut(it, val, true)
		}
	}
	if lastResult != nil {
		it.primaryIt.Contains(lastResult)
	}
	return graph.ContainsLogOut(it, val, false)
}

// Returns the approximate size of the And iterator. Because we're dealing
// with an intersection, we know that the largest we can be is the size of the
// smallest iterator. This is the heuristic we shall follow. Better heuristics
// welcome.
func (it *And) Size() (int64, bool) {
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

// An And has no NextPath of its own -- that is, there are no other values
// which satisfy our previous result that are not the result itself. Our
// subiterators might, however, so just pass the call recursively.
func (it *And) NextPath() bool {
	if it.primaryIt.NextPath() {
		return true
	}
	for _, sub := range it.internalIterators {
		if sub.NextPath() {
			return true
		}
	}
	return false
}

// Perform and-specific cleanup, of which there currently is none.
func (it *And) cleanUp() {}

// Close this iterator, and, by extension, close the subiterators.
// Close should be idempotent, and it follows that if it's subiterators
// follow this contract, the And follows the contract.
func (it *And) Close() {
	it.cleanUp()
	it.primaryIt.Close()
	for _, sub := range it.internalIterators {
		sub.Close()
	}
}

// Register this as an "and" iterator.
func (it *And) Type() graph.Type { return graph.And }
