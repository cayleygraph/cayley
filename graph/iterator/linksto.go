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

// Defines one of the base iterators, the LinksTo iterator. A LinksTo takes a
// subiterator of nodes, and contains an iteration of links which "link to"
// those nodes in a given direction.
//
// Next()ing a LinksTo is straightforward -- iterate through all links to //
// things in the subiterator, and then advance the subiterator, and do it again.
// LinksTo is therefore sensitive to growing with a fanout. (A small-sized
// subiterator could cause LinksTo to be large).
//
// Contains()ing a LinksTo means, given a link, take the direction we care about
// and check if it's in our subiterator. Checking is therefore fairly cheap, and
// similar to checking the subiterator alone.
//
// Can be seen as the dual of the HasA iterator.

import (
	"context"
	"fmt"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
)

var _ graph.Iterator = &LinksTo{}

// A LinksTo has a reference back to the graph.QuadStore (to create the iterators
// for each node) the subiterator, and the direction the iterator comes from.
// `next_it` is the tempoarary iterator held per result in `primary_it`.
type LinksTo struct {
	uid       uint64
	qs        graph.QuadStore
	primaryIt graph.Iterator
	dir       quad.Direction
	nextIt    graph.Iterator
	result    graph.Value
	runstats  graph.IteratorStats
	err       error
}

// Construct a new LinksTo iterator around a direction and a subiterator of
// nodes.
func NewLinksTo(qs graph.QuadStore, it graph.Iterator, d quad.Direction) *LinksTo {
	return &LinksTo{
		uid:       NextUID(),
		qs:        qs,
		primaryIt: it,
		dir:       d,
		nextIt:    &Null{},
	}
}

func (it *LinksTo) UID() uint64 {
	return it.uid
}

func (it *LinksTo) Reset() {
	it.primaryIt.Reset()
	if it.nextIt != nil {
		it.nextIt.Close()
	}
	it.nextIt = &Null{}
}

func (it *LinksTo) Clone() graph.Iterator {
	out := NewLinksTo(it.qs, it.primaryIt.Clone(), it.dir)
	out.runstats.Size, out.runstats.ExactSize = it.runstats.Size, it.runstats.ExactSize
	return out
}

// Return the direction under consideration.
func (it *LinksTo) Direction() quad.Direction { return it.dir }

// Tag these results, and our subiterator's results.
func (it *LinksTo) TagResults(dst map[string]graph.Value) {
	it.primaryIt.TagResults(dst)
}

func (it *LinksTo) String() string {
	return fmt.Sprintf("LinksTo(%v)", it.dir)
}

// If it checks in the right direction for the subiterator, it is a valid link
// for the LinksTo.
func (it *LinksTo) Contains(ctx context.Context, val graph.Value) bool {
	it.runstats.Contains += 1
	node := it.qs.QuadDirection(val, it.dir)
	if it.primaryIt.Contains(ctx, node) {
		it.result = val
		return true
	}
	it.err = it.primaryIt.Err()
	return false
}

// Return a list containing only our subiterator.
func (it *LinksTo) SubIterators() []graph.Iterator {
	return []graph.Iterator{it.primaryIt}
}

// Optimize the LinksTo, by replacing it if it can be.
func (it *LinksTo) Optimize() (graph.Iterator, bool) {
	newPrimary, changed := it.primaryIt.Optimize()
	if changed {
		it.primaryIt = newPrimary
		if it.primaryIt.Type() == graph.Null {
			it.nextIt.Close()
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

// Next()ing a LinksTo operates as described above.
func (it *LinksTo) Next(ctx context.Context) bool {
	for {
		it.runstats.Next += 1
		if it.nextIt.Next(ctx) {
			it.runstats.ContainsNext += 1
			it.result = it.nextIt.Result()
			return true
		}

		// If there's an error in the 'next' iterator, we save it and we're done.
		it.err = it.nextIt.Err()
		if it.err != nil {
			return false
		}

		// Subiterator is empty, get another one
		if !it.primaryIt.Next(ctx) {
			// Possibly save error
			it.err = it.primaryIt.Err()

			// We're out of nodes in our subiterator, so we're done as well.
			return false
		}
		it.nextIt.Close()
		it.nextIt = it.qs.QuadIterator(it.dir, it.primaryIt.Result())

		// Continue -- return the first in the next set.
	}
}

func (it *LinksTo) Err() error {
	return it.err
}

func (it *LinksTo) Result() graph.Value {
	return it.result
}

// Close closes the iterator.  It closes all subiterators it can, but
// returns the first error it encounters.
func (it *LinksTo) Close() error {
	err := it.nextIt.Close()

	_err := it.primaryIt.Close()
	if _err != nil && err == nil {
		err = _err
	}

	return err
}

// We won't ever have a new result, but our subiterators might.
func (it *LinksTo) NextPath(ctx context.Context) bool {
	ok := it.primaryIt.NextPath(ctx)
	if !ok {
		it.err = it.primaryIt.Err()
	}
	return ok
}

// Register the LinksTo.
func (it *LinksTo) Type() graph.Type { return graph.LinksTo }

// Return a guess as to how big or costly it is to next the iterator.
func (it *LinksTo) Stats() graph.IteratorStats {
	subitStats := it.primaryIt.Stats()
	// TODO(barakmich): These should really come from the quadstore itself
	checkConstant := int64(1)
	nextConstant := int64(2)
	st := graph.IteratorStats{
		NextCost:     nextConstant + subitStats.NextCost,
		ContainsCost: checkConstant + subitStats.ContainsCost,
		Next:         it.runstats.Next,
		Contains:     it.runstats.Contains,
		ContainsNext: it.runstats.ContainsNext,
	}
	st.Size, st.ExactSize = it.Size()
	return st
}

func (it *LinksTo) Size() (int64, bool) {
	if it.runstats.Size != 0 {
		return it.runstats.Size, it.runstats.ExactSize
	}
	if fixed, ok := it.primaryIt.(*Fixed); ok {
		// get real sizes from sub iterators
		var (
			sz    int64
			exact = true
		)
		for _, v := range fixed.Values() {
			sit := it.qs.QuadIterator(it.dir, v)
			n, ex := sit.Size()
			sit.Close()
			sz += n
			exact = exact && ex
		}
		it.runstats.Size, it.runstats.ExactSize = sz, exact
		return sz, exact
	}
	// TODO(barakmich): It should really come from the quadstore itself
	const fanoutFactor = 20
	sz, _ := it.primaryIt.Size()
	sz *= fanoutFactor
	it.runstats.Size, it.runstats.ExactSize = sz, false
	return sz, false
}
