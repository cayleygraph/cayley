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

	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/quad"
)

// A LinksTo has a reference back to the graph.QuadStore (to create the iterators
// for each node) the subiterator, and the direction the iterator comes from.
// `next_it` is the tempoarary iterator held per result in `primary_it`.
type LinksTo struct {
	qs      QuadIndexer
	primary iterator.Shape
	dir     quad.Direction
	size    refs.Size
}

// NewLinksTo construct a new LinksTo iterator around a direction and a subiterator of
// nodes.
func NewLinksTo(qs QuadIndexer, it iterator.Shape, d quad.Direction) *LinksTo {
	return &LinksTo{
		qs:      qs,
		primary: it,
		dir:     d,
	}
}

// Direction returns the direction under consideration.
func (it *LinksTo) Direction() quad.Direction { return it.dir }

func (it *LinksTo) Iterate() iterator.Scanner {
	return newLinksToNext(it.qs, it.primary.Iterate(), it.dir)
}

func (it *LinksTo) Lookup() iterator.Index {
	return newLinksToContains(it.qs, it.primary.Lookup(), it.dir)
}

func (it *LinksTo) String() string {
	return fmt.Sprintf("LinksTo(%v)", it.dir)
}

// SubIterators returns a list containing only our subiterator.
func (it *LinksTo) SubIterators() []iterator.Shape {
	return []iterator.Shape{it.primary}
}

// Optimize the LinksTo, by replacing it if it can be.
func (it *LinksTo) Optimize(ctx context.Context) (iterator.Shape, bool) {
	newPrimary, changed := it.primary.Optimize(ctx)
	if changed {
		it.primary = newPrimary
		if iterator.IsNull(it.primary) {
			return it.primary, true
		}
	}
	return it, false
}

// Stats returns a guess as to how big or costly it is to next the iterator.
func (it *LinksTo) Stats(ctx context.Context) (iterator.Costs, error) {
	subitStats, err := it.primary.Stats(ctx)
	// TODO(barakmich): These should really come from the quadstore itself
	checkConstant := int64(1)
	nextConstant := int64(2)
	return iterator.Costs{
		NextCost:     nextConstant + subitStats.NextCost,
		ContainsCost: checkConstant + subitStats.ContainsCost,
		Size:         it.getSize(ctx),
	}, err
}

func (it *LinksTo) getSize(ctx context.Context) refs.Size {
	if it.size.Value != 0 {
		return it.size
	}
	if fixed, ok := it.primary.(*iterator.Fixed); ok {
		// get real sizes from sub iterators
		var (
			sz    int64
			exact = true
		)
		for _, v := range fixed.Values() {
			sit := it.qs.QuadIterator(it.dir, v)
			st, _ := sit.Stats(ctx)
			sz += st.Size.Value
			exact = exact && st.Size.Exact
		}
		it.size.Value, it.size.Exact = sz, exact
		return it.size
	}
	stats, _ := it.qs.Stats(ctx, false)
	maxSize := stats.Quads.Value/2 + 1
	// TODO(barakmich): It should really come from the quadstore itself
	const fanoutFactor = 20
	st, _ := it.primary.Stats(ctx)
	value := st.Size.Value * fanoutFactor
	if value > maxSize {
		value = maxSize
	}
	it.size.Value, it.size.Exact = value, false
	return it.size
}

// A LinksTo has a reference back to the graph.QuadStore (to create the iterators
// for each node) the subiterator, and the direction the iterator comes from.
// `next_it` is the tempoarary iterator held per result in `primary_it`.
type linksToNext struct {
	qs      QuadIndexer
	primary iterator.Scanner
	dir     quad.Direction
	nextIt  iterator.Scanner
	result  refs.Ref
	err     error
}

// Construct a new LinksTo iterator around a direction and a subiterator of
// nodes.
func newLinksToNext(qs QuadIndexer, it iterator.Scanner, d quad.Direction) iterator.Scanner {
	return &linksToNext{
		qs:      qs,
		primary: it,
		dir:     d,
		nextIt:  iterator.NewNull().Iterate(),
	}
}

// Return the direction under consideration.
func (it *linksToNext) Direction() quad.Direction { return it.dir }

// Tag these results, and our subiterator's results.
func (it *linksToNext) TagResults(dst map[string]refs.Ref) {
	it.primary.TagResults(dst)
}

func (it *linksToNext) String() string {
	return fmt.Sprintf("LinksToNext(%v)", it.dir)
}

// Next()ing a LinksTo operates as described above.
func (it *linksToNext) Next(ctx context.Context) bool {
	for {
		if it.nextIt.Next(ctx) {
			it.result = it.nextIt.Result()
			return true
		}

		// If there's an error in the 'next' iterator, we save it and we're done.
		it.err = it.nextIt.Err()
		if it.err != nil {
			return false
		}

		// Subiterator is empty, get another one
		if !it.primary.Next(ctx) {
			// Possibly save error
			it.err = it.primary.Err()

			// We're out of nodes in our subiterator, so we're done as well.
			return false
		}
		it.nextIt.Close()
		it.nextIt = it.qs.QuadIterator(it.dir, it.primary.Result()).Iterate()

		// Continue -- return the first in the next set.
	}
}

func (it *linksToNext) Err() error {
	return it.err
}

func (it *linksToNext) Result() refs.Ref {
	return it.result
}

// Close closes the iterator.  It closes all subiterators it can, but
// returns the first error it encounters.
func (it *linksToNext) Close() error {
	err := it.nextIt.Close()

	_err := it.primary.Close()
	if _err != nil && err == nil {
		err = _err
	}

	return err
}

// We won't ever have a new result, but our subiterators might.
func (it *linksToNext) NextPath(ctx context.Context) bool {
	ok := it.primary.NextPath(ctx)
	if !ok {
		it.err = it.primary.Err()
	}
	return ok
}

// A LinksTo has a reference back to the graph.QuadStore (to create the iterators
// for each node) the subiterator, and the direction the iterator comes from.
// `next_it` is the tempoarary iterator held per result in `primary_it`.
type linksToContains struct {
	qs      QuadIndexer
	primary iterator.Index
	dir     quad.Direction
	result  refs.Ref
}

// Construct a new LinksTo iterator around a direction and a subiterator of
// nodes.
func newLinksToContains(qs QuadIndexer, it iterator.Index, d quad.Direction) iterator.Index {
	return &linksToContains{
		qs:      qs,
		primary: it,
		dir:     d,
	}
}

// Return the direction under consideration.
func (it *linksToContains) Direction() quad.Direction { return it.dir }

// Tag these results, and our subiterator's results.
func (it *linksToContains) TagResults(dst map[string]refs.Ref) {
	it.primary.TagResults(dst)
}

func (it *linksToContains) String() string {
	return fmt.Sprintf("LinksToContains(%v)", it.dir)
}

// If it checks in the right direction for the subiterator, it is a valid link
// for the LinksTo.
func (it *linksToContains) Contains(ctx context.Context, val refs.Ref) bool {
	node := it.qs.QuadDirection(val, it.dir)
	if it.primary.Contains(ctx, node) {
		it.result = val
		return true
	}
	return false
}

func (it *linksToContains) Err() error {
	return it.primary.Err()
}

func (it *linksToContains) Result() refs.Ref {
	return it.result
}

// Close closes the iterator.  It closes all subiterators it can, but
// returns the first error it encounters.
func (it *linksToContains) Close() error {
	return it.primary.Close()
}

// We won't ever have a new result, but our subiterators might.
func (it *linksToContains) NextPath(ctx context.Context) bool {
	return it.primary.NextPath(ctx)
}
