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
	"github.com/cayleygraph/cayley/clog"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
)

// A HasA consists of a reference back to the graph.QuadStore that it references,
// a primary subiterator, a direction in which the quads for that subiterator point,
// and a temporary holder for the iterator generated on Contains().
type HasA struct {
	uid       uint64
	tags      graph.Tagger
	qs        graph.QuadStore
	primaryIt graph.Iterator
	dir       quad.Direction
	resultIt  graph.Iterator
	result    graph.Value
	runstats  graph.IteratorStats
	err       error
}

// Construct a new HasA iterator, given the quad subiterator, and the quad
// direction for which it stands.
func NewHasA(qs graph.QuadStore, subIt graph.Iterator, d quad.Direction) *HasA {
	return &HasA{
		uid:       NextUID(),
		qs:        qs,
		primaryIt: subIt,
		dir:       d,
	}
}

func (it *HasA) UID() uint64 {
	return it.uid
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

func (it *HasA) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *HasA) Clone() graph.Iterator {
	out := NewHasA(it.qs, it.primaryIt.Clone(), it.dir)
	out.tags.CopyFrom(it)
	return out
}

// Direction accessor.
func (it *HasA) Direction() quad.Direction { return it.dir }

// Pass the Optimize() call along to the subiterator. If it becomes Null,
// then the HasA becomes Null (there are no quads that have any directions).
func (it *HasA) Optimize() (graph.Iterator, bool) {
	newPrimary, changed := it.primaryIt.Optimize()
	if changed {
		it.primaryIt = newPrimary
		if it.primaryIt.Type() == graph.Null {
			return it.primaryIt, true
		}
	}
	// Ask the graph.QuadStore if we can be replaced. Often times, this is a great
	// optimization opportunity (there's a fixed iterator underneath us, for
	// example).
	newReplacement, hasOne := it.qs.OptimizeIterator(it)
	if hasOne {
		it.Close()
		return newReplacement, true
	}
	return it, false
}

// Pass the TagResults down the chain.
func (it *HasA) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}

	it.primaryIt.TagResults(dst)
}

func (it *HasA) Describe() graph.Description {
	primary := it.primaryIt.Describe()
	return graph.Description{
		UID:       it.UID(),
		Type:      it.Type(),
		Tags:      it.tags.Tags(),
		Direction: it.dir,
		Iterator:  &primary,
	}
}

// Check a value against our internal iterator. In order to do this, we must first open a new
// iterator of "quads that have `val` in our direction", given to us by the quad store,
// and then Next() values out of that iterator and Contains() them against our subiterator.
func (it *HasA) Contains(val graph.Value) bool {
	graph.ContainsLogIn(it, val)
	it.runstats.Contains += 1
	if clog.V(4) {
		clog.Infof("Id is %v", it.qs.NameOf(val))
	}
	// TODO(barakmich): Optimize this
	if it.resultIt != nil {
		it.resultIt.Close()
	}
	it.resultIt = it.qs.QuadIterator(it.dir, val)
	ok := it.NextContains()
	if it.err != nil {
		return false
	}
	return graph.ContainsLogOut(it, val, ok)
}

// NextContains() is shared code between Contains() and GetNextResult() -- calls next on the
// result iterator (a quad iterator based on the last checked value) and returns true if
// another match is made.
func (it *HasA) NextContains() bool {
	for it.resultIt.Next() {
		it.runstats.ContainsNext += 1
		link := it.resultIt.Result()
		if clog.V(4) {
			clog.Infof("Quad is %v", it.qs.Quad(link))
		}
		if it.primaryIt.Contains(link) {
			it.result = it.qs.QuadDirection(link, it.dir)
			return true
		}
	}
	it.err = it.resultIt.Err()
	return false
}

// Get the next result that matches this branch.
func (it *HasA) NextPath() bool {
	// Order here is important. If the subiterator has a NextPath, then we
	// need do nothing -- there is a next result, and we shouldn't move forward.
	// However, we then need to get the next result from our last Contains().
	//
	// The upshot is, the end of NextPath() bubbles up from the bottom of the
	// iterator tree up, and we need to respect that.
	if clog.V(4) {
		clog.Infof("HASA %v NextPath", it.UID())
	}
	if it.primaryIt.NextPath() {
		return true
	}
	it.err = it.primaryIt.Err()
	if it.err != nil {
		return false
	}

	result := it.NextContains() // Sets it.err if there's an error
	if it.err != nil {
		return false
	}
	if clog.V(4) {
		clog.Infof("HASA %v NextPath Returns %v", it.UID(), result)
	}
	return result
}

// Next advances the iterator. This is simpler than Contains. We have a
// subiterator we can get a value from, and we can take that resultant quad,
// pull our direction out of it, and return that.
func (it *HasA) Next() bool {
	graph.NextLogIn(it)
	it.runstats.Next += 1
	if it.resultIt != nil {
		it.resultIt.Close()
	}
	it.resultIt = &Null{}

	if !it.primaryIt.Next() {
		it.err = it.primaryIt.Err()
		return graph.NextLogOut(it, false)
	}
	tID := it.primaryIt.Result()
	val := it.qs.QuadDirection(tID, it.dir)
	it.result = val
	return graph.NextLogOut(it, true)
}

func (it *HasA) Err() error {
	return it.err
}

func (it *HasA) Result() graph.Value {
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

// Register this iterator as a HasA.
func (it *HasA) Type() graph.Type { return graph.HasA }

func (it *HasA) Size() (int64, bool) {
	st := it.Stats()
	return st.Size, st.ExactSize
}

var _ graph.Iterator = &HasA{}
