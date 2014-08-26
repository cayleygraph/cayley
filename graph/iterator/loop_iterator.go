package iterator

import (
	"fmt"
	"strings"

	"github.com/google/cayley/graph"
)

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

func (it *Loop) Contains(val graph.Value) bool {
	graph.ContainsLogIn(it, val)
	if it.loopIt.Contains(val) {
		return graph.ContainsLogOut(it, val, true)
	}
	return graph.ContainsLogOut(it, val, false)
}

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

func (it *Loop) checkFilter(value graph.Value) bool {
	fixed := it.ts.FixedIterator()
	fixed.Add(value)

	it.filterEntryIt.SetIterator(fixed)
	it.filterIt.Reset()

	//fmt.Println("Before add value")
	//it.filterEntryIt.Add(value)
	//fmt.Println("After add value")

	//fmt.Println("Before next")
	answer := graph.Next(it.filterIt)
	//fmt.Println("After next")
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
			// A value has not been found, try looping again
			it.advanceLoop()
			return it.next()
		}

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

func (it *Loop) NextPath() bool {
	return it.loopIt.NextPath()
}

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

func (it *Loop) SubIterators() []graph.Iterator {
	return []graph.Iterator{it.baseIt, it.loopIt}
}

func (it *Loop) DebugString(indent int) string {
	return fmt.Sprintf("%s(%s %d \n%s)",
		strings.Repeat(" ", indent),
		it.Type(), it.UID(), it.baseIt.DebugString(indent+4))
}

func (it *Loop) Close() {
	it.baseIt.Close()
	it.loopIt.Close()
}

// DEPRECATED
func (it *Loop) ResultTree() *graph.ResultTree {
	return graph.NewResultTree(it.Result())
}
