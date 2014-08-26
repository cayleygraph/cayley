package iterator

import (
	"fmt"
	"strings"

	"github.com/google/cayley/graph"
)

// Loop implements a loop operator. Composed of the following members:
// - baseIt - the iterator composing the query preceding the loop.
// - loopIt - an iterator implementing the loop morph query; has an EntryPoint iterator as starting point.
// - loopEntryIt - the starting point for the loop iterator; knowing this allows us to plug and change the source iterator
// - filterIt - an iterator implementing the filtering part of the loop; has an EntryPoint iterator as starting point
// - filterEntryIt - the starting point for the filter iterator; allows to plug and change the source iterator
// - prevValuesIt - the results obtained for each loop iteration will be stored in this iterator;
//					this allows us to use this iterator as source for the next loop
type Loop struct {
	uid            uint64
	tags           graph.Tagger
	ts             graph.TripleStore
	baseIt         graph.Iterator
	loopIt         graph.Iterator
	loopEntryIt    *EntryPoint
	filterIt       graph.Iterator
	filterEntryIt  *EntryPoint
	result         graph.Value
	runstats       graph.IteratorStats
	prevValuesIt   graph.FixedIterator
	loops          int
	bounded        bool
	loopsCompleted int
	finished       bool
}

func NewLoop(ts graph.TripleStore, baseIt, loopIt, filterIt graph.Iterator, loopEntryIt, filterEntryIt *EntryPoint, loops int, bounded bool) *Loop {
	return &Loop{
		uid:            NextUID(),
		ts:             ts,
		baseIt:         baseIt,
		loopEntryIt:    loopEntryIt,
		loopIt:         loopIt,
		filterEntryIt:  filterEntryIt,
		filterIt:       filterIt,
		prevValuesIt:   ts.FixedIterator(),
		loops:          loops,
		bounded:        bounded,
		loopsCompleted: 0,
		finished:       false,
	}
}

func (it *Loop) UID() uint64 {
	return it.uid
}

func (it *Loop) Tagger() *graph.Tagger {
	return &it.tags
}

// TODO
func (it *Loop) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}

	it.baseIt.TagResults(dst)
	it.loopIt.TagResults(dst)
}

// TODO
func (it *Loop) Contains(val graph.Value) bool {
	graph.ContainsLogIn(it, val)
	if it.loopIt.Contains(val) {
		return graph.ContainsLogOut(it, val, true)
	}
	return graph.ContainsLogOut(it, val, false)
}

// TODO
func (it *Loop) Clone() graph.Iterator {
	out := NewLoop(it.ts, it.baseIt, it.loopIt, it.filterIt, it.loopEntryIt, it.filterEntryIt, it.loops, it.bounded)
	out.tags.CopyFrom(it)
	return out
}

func (it *Loop) Type() graph.Type { return graph.Loop }

func (it *Loop) Reset() {
	// Reset the iterators
	it.baseIt.Reset()
	it.loopEntryIt.SetIterator(it.baseIt)
	it.loopIt.Reset()
	it.prevValuesIt.Close()
	it.prevValuesIt = it.ts.FixedIterator()

	// Reset the state
	it.loopsCompleted = 0
	it.finished = false
	it.result = nil
}

func (it *Loop) advanceLoop() {
	// Set the loop iterator to feed from the previous iteration results
	it.loopEntryIt.SetIterator(it.prevValuesIt)

	// Reset the loop iterator - will also clean the values in the underlying fixed iterator.
	it.loopIt.Reset()

	it.filterIt.Reset()

	// Increment the completed loops count
	it.loopsCompleted += 1

	// Mark the loop as finished - no more results can be expected.
	// Either the number of loops has been executed, or there are
	// no more expandable nodes.
	if size, _ := it.prevValuesIt.Size(); (it.bounded && it.loopsCompleted >= it.loops) || size == 0 {
		it.finished = true
	}

	// Clean the set of values seen in the previous loop
	it.prevValuesIt = it.ts.FixedIterator()
}

// checkFilter checks whether a value is expandable using the filter iterator.
func (it *Loop) checkFilter(value graph.Value) bool {
	// Create a fixed iterator containing the value
	fixed := it.ts.FixedIterator()
	fixed.Add(value)

	// Set is as the source for the filter iterator.
	it.filterEntryIt.SetIterator(fixed)
	it.filterIt.Reset()

	// Check if the filter has a next value.
	answer := graph.Next(it.filterIt)
	return answer
}

func (it *Loop) Next() bool {
	graph.NextLogIn(it)
	it.runstats.Next += 1

	return it.next()
}

func (it *Loop) next() bool {
	// Check if the loop has any more results
	if it.finished {
		return graph.NextLogOut(it, nil, false)
	}

	for i := 0; ; i++ {
		if found := graph.Next(it.loopIt); !found {
			// A value has not been found, try a new loop iteration.
			it.advanceLoop()
			return it.next()
		}

		// For a found value, we must check it passes the filter.
		if it.checkFilter(it.loopIt.Result()) {
			// A value has been found.
			it.result = it.loopIt.Result()
			it.prevValuesIt.Add(it.result)
			it.runstats.ContainsNext += 1

			return graph.NextLogOut(it, it.result, true)
		}
	}
}

func (it *Loop) Result() graph.Value {
	return it.result
}

// TODO
func (it *Loop) NextPath() bool {
	return it.loopIt.NextPath()
}

// TODO
func (it *Loop) Stats() graph.IteratorStats {
	subitStats := it.loopIt.Stats()
	// TODO(barakmich): These should really come from the triplestore itself
	fanoutFactor := int64(20)
	checkConstant := int64(1)
	nextConstant := int64(2)
	return graph.IteratorStats{
		NextCost:     nextConstant + subitStats.NextCost,
		ContainsCost: checkConstant + subitStats.ContainsCost,
		Size:         fanoutFactor * subitStats.Size,
		Next:         it.runstats.Next,
		Contains:     it.runstats.Contains,
		ContainsNext: it.runstats.ContainsNext,
	}
}

func (it *Loop) Size() (int64, bool) {
	return it.Stats().Size, false
}

// TODO
func (it *Loop) Optimize() (graph.Iterator, bool) {
	return it, false
}

// TODO
func (it *Loop) SubIterators() []graph.Iterator {
	return []graph.Iterator{it.baseIt, it.loopIt}
}

// TODO
func (it *Loop) DebugString(indent int) string {
	return fmt.Sprintf("%s(%s %d \n%s)",
		strings.Repeat(" ", indent),
		it.Type(), it.UID(), it.baseIt.DebugString(indent+4))
}

func (it *Loop) Close() {
	it.baseIt.Close()
	it.loopIt.Close()
	it.filterIt.Close()
}

// DEPRECATED
func (it *Loop) ResultTree() *graph.ResultTree {
	return graph.NewResultTree(it.Result())
}
