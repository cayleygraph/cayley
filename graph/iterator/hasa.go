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

// Defines one of the base iterators, the HasA iterator. The HasA takes a
// subiterator of links, and acts as an iterator of nodes in the given
// direction. The name comes from the idea that a "link HasA subject" or a "link
// HasA predicate".
//
// HasA is weird in that it may return the same value twice if on the Next()
// path. That's okay -- in reality, it can be viewed as returning the value for
// a new quad, but to make logic much simpler, here we have the HasA.
//
// Likewise, it's important to think about Contains()ing a HasA. When given a
// value to check, it means "Check all predicates that have this value for your
// direction against the subiterator." This would imply that there's more than
// one possibility for the same Contains()ed value. While we could return the
// number of options, it's simpler to return one, and then call NextPath()
// enough times to enumerate the options. (In fact, one could argue that the
// raison d'etre for NextPath() is this iterator).
//
// Alternatively, can be seen as the dual of the LinksTo iterator.

import (
	"context"
	"fmt"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
)

var _ graph.Iterator = &HasA{}

// A HasA consists of a reference back to the graph.QuadStore that it references,
// a primary subiterator, a direction in which the quads for that subiterator point,
// and a temporary holder for the iterator generated on Contains().
type HasA struct {
	qs        graph.QuadIndexer
	primaryIt graph.Iterator
	dir       quad.Direction
	resultIt  graph.Iterator
	result    graph.Ref
	runstats  graph.IteratorStats
	err       error
}

// Construct a new HasA iterator, given the quad subiterator, and the quad
// direction for which it stands.
func NewHasA(qs graph.QuadIndexer, subIt graph.Iterator, d quad.Direction) *HasA {
	return &HasA{
		qs:        qs,
		primaryIt: subIt,
		dir:       d,
	}
}

// Return our sole subiterator.
func (it *HasA) SubIterators() []graph.Iterator {
	return []graph.Iterator{it.primaryIt}
}

func (it *HasA) Reset() {
	it.primaryIt.Reset()
	if it.resultIt != nil {
		it.resultIt.Close()
	}
}

// Direction accessor.
func (it *HasA) Direction() quad.Direction { return it.dir }

// Pass the Optimize() call along to the subiterator. If it becomes Null,
// then the HasA becomes Null (there are no quads that have any directions).
func (it *HasA) Optimize() (graph.Iterator, bool) {
	newPrimary, changed := it.primaryIt.Optimize()
	if changed {
		it.primaryIt = newPrimary
		if _, ok := it.primaryIt.(*Null); ok {
			return it.primaryIt, true
		}
	}
	return it, false
}

// Pass the TagResults down the chain.
func (it *HasA) TagResults(dst map[string]graph.Ref) {
	it.primaryIt.TagResults(dst)
}

func (it *HasA) String() string {
	return fmt.Sprintf("HasA(%v)", it.dir)
}

// Check a value against our internal iterator. In order to do this, we must first open a new
// iterator of "quads that have `val` in our direction", given to us by the quad store,
// and then Next() values out of that iterator and Contains() them against our subiterator.
func (it *HasA) Contains(ctx context.Context, val graph.Ref) bool {
	it.runstats.Contains += 1
	if clog.V(4) {
		clog.Infof("Id is %v", val)
	}
	// TODO(barakmich): Optimize this
	if it.resultIt != nil {
		it.resultIt.Close()
	}
	it.resultIt = it.qs.QuadIterator(it.dir, val)
	ok := it.NextContains(ctx)
	if it.err != nil {
		return false
	}
	return ok
}

// NextContains() is shared code between Contains() and GetNextResult() -- calls next on the
// result iterator (a quad iterator based on the last checked value) and returns true if
// another match is made.
func (it *HasA) NextContains(ctx context.Context) bool {
	if it.resultIt == nil {
		return false
	}
	for it.resultIt.Next(ctx) {
		it.runstats.ContainsNext += 1
		link := it.resultIt.Result()
		if clog.V(4) {
			clog.Infof("Quad is %v", it.qs.Quad(link))
		}
		// we expect this to reset the iterator if we were Next'ing
		if it.primaryIt.Contains(ctx, link) {
			it.result = it.qs.QuadDirection(link, it.dir)
			return true
		}
	}
	it.err = it.resultIt.Err()
	return false
}

// Get the next result that matches this branch.
func (it *HasA) NextPath(ctx context.Context) bool {
	// Order here is important. If the subiterator has a NextPath, then we
	// need do nothing -- there is a next result, and we shouldn't move forward.
	// However, we then need to get the next result from our last Contains().
	//
	// The upshot is, the end of NextPath() bubbles up from the bottom of the
	// iterator tree up, and we need to respect that.
	if clog.V(4) {
		clog.Infof("HASA %p NextPath", it)
	}
	if it.primaryIt.NextPath(ctx) {
		return true
	}
	it.err = it.primaryIt.Err()
	if it.err != nil {
		return false
	}

	result := it.NextContains(ctx) // Sets it.err if there's an error
	if it.err != nil {
		return false
	}
	if clog.V(4) {
		clog.Infof("HASA %p NextPath Returns %v", it, result)
	}
	return result
}

// Next advances the iterator. This is simpler than Contains. We have a
// subiterator we can get a value from, and we can take that resultant quad,
// pull our direction out of it, and return that.
func (it *HasA) Next(ctx context.Context) bool {
	it.runstats.Next += 1
	if it.resultIt != nil {
		it.resultIt.Close()
	}

	if !it.primaryIt.Next(ctx) {
		it.err = it.primaryIt.Err()
		return false
	}
	tID := it.primaryIt.Result()
	val := it.qs.QuadDirection(tID, it.dir)
	it.result = val
	return true
}

func (it *HasA) Err() error {
	return it.err
}

func (it *HasA) Result() graph.Ref {
	return it.result
}

// GetStats() returns the statistics on the HasA iterator. This is curious. Next
// cost is easy, it's an extra call or so on top of the subiterator Next cost.
// ContainsCost involves going to the graph.QuadStore, iterating out values, and hoping
// one sticks -- potentially expensive, depending on fanout. Size, however, is
// potentially smaller. we know at worst it's the size of the subiterator, but
// if there are many repeated values, it could be much smaller in totality.
func (it *HasA) Stats() graph.IteratorStats {
	subitStats := it.primaryIt.Stats()
	// TODO(barakmich): These should really come from the quadstore itself
	// and be optimized.
	faninFactor := int64(1)
	fanoutFactor := int64(30)
	nextConstant := int64(2)
	quadConstant := int64(1)
	return graph.IteratorStats{
		NextCost:     quadConstant + subitStats.NextCost,
		ContainsCost: (fanoutFactor * nextConstant) * subitStats.ContainsCost,
		Size:         faninFactor * subitStats.Size,
		ExactSize:    false,
		Next:         it.runstats.Next,
		Contains:     it.runstats.Contains,
		ContainsNext: it.runstats.ContainsNext,
	}
}

// Close the subiterator, the result iterator (if any) and the HasA. It closes
// all subiterators it can, but returns the first error it encounters.
func (it *HasA) Close() error {
	err := it.primaryIt.Close()

	if it.resultIt != nil {
		_err := it.resultIt.Close()
		if err == nil {
			err = _err
		}
	}

	return err
}

func (it *HasA) Size() (int64, bool) {
	st := it.Stats()
	return st.Size, st.ExactSize
}
