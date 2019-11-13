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
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/quad"
)

// A HasA consists of a reference back to the graph.QuadStore that it references,
// a primary subiterator, a direction in which the quads for that subiterator point,
// and a temporary holder for the iterator generated on Contains().
type HasA struct {
	qs      QuadIndexer
	primary iterator.Shape
	dir     quad.Direction
}

// NewHasA construct a new HasA iterator, given the quad subiterator, and the quad
// direction for which it stands.
func NewHasA(qs QuadIndexer, subIt iterator.Shape, d quad.Direction) *HasA {
	return &HasA{
		qs:      qs,
		primary: subIt,
		dir:     d,
	}
}

func (it *HasA) Iterate() iterator.Scanner {
	return newHasANext(it.qs, it.primary.Iterate(), it.dir)
}

func (it *HasA) Lookup() iterator.Index {
	return newHasAContains(it.qs, it.primary.Lookup(), it.dir)
}

// SubIterators returns our sole subiterator.
func (it *HasA) SubIterators() []iterator.Shape {
	return []iterator.Shape{it.primary}
}

// Direction accessor.
func (it *HasA) Direction() quad.Direction { return it.dir }

// Optimize pass the Optimize() call along to the subiterator. If it becomes Null,
// then the HasA becomes Null (there are no quads that have any directions).
func (it *HasA) Optimize(ctx context.Context) (iterator.Shape, bool) {
	newPrimary, changed := it.primary.Optimize(ctx)
	if changed {
		it.primary = newPrimary
		if iterator.IsNull(it.primary) {
			return it.primary, true
		}
	}
	return it, false
}

func (it *HasA) String() string {
	return fmt.Sprintf("HasA(%v)", it.dir)
}

// Stats returns the statistics on the HasA iterator. This is curious. Next
// cost is easy, it's an extra call or so on top of the subiterator Next cost.
// ContainsCost involves going to the graph.QuadStore, iterating out values, and hoping
// one sticks -- potentially expensive, depending on fanout. Size, however, is
// potentially smaller. we know at worst it's the size of the subiterator, but
// if there are many repeated values, it could be much smaller in totality.
func (it *HasA) Stats(ctx context.Context) (iterator.Costs, error) {
	subitStats, err := it.primary.Stats(ctx)
	// TODO(barakmich): These should really come from the quadstore itself
	// and be optimized.
	faninFactor := int64(1)
	fanoutFactor := int64(30)
	nextConstant := int64(2)
	quadConstant := int64(1)
	return iterator.Costs{
		NextCost:     quadConstant + subitStats.NextCost,
		ContainsCost: (fanoutFactor * nextConstant) * subitStats.ContainsCost,
		Size: refs.Size{
			Value: faninFactor * subitStats.Size.Value,
			Exact: false,
		},
	}, err
}

// A HasA consists of a reference back to the graph.QuadStore that it references,
// a primary subiterator, a direction in which the quads for that subiterator point,
// and a temporary holder for the iterator generated on Contains().
type hasANext struct {
	qs      QuadIndexer
	primary iterator.Scanner
	dir     quad.Direction
	result  refs.Ref
}

// Construct a new HasA iterator, given the quad subiterator, and the quad
// direction for which it stands.
func newHasANext(qs QuadIndexer, subIt iterator.Scanner, d quad.Direction) *hasANext {
	return &hasANext{
		qs:      qs,
		primary: subIt,
		dir:     d,
	}
}

// Direction accessor.
func (it *hasANext) Direction() quad.Direction { return it.dir }

// Pass the TagResults down the chain.
func (it *hasANext) TagResults(dst map[string]refs.Ref) {
	it.primary.TagResults(dst)
}

func (it *hasANext) String() string {
	return fmt.Sprintf("HasANext(%v)", it.dir)
}

// Get the next result that matches this branch.
func (it *hasANext) NextPath(ctx context.Context) bool {
	return it.primary.NextPath(ctx)
}

// Next advances the iterator. This is simpler than Contains. We have a
// subiterator we can get a value from, and we can take that resultant quad,
// pull our direction out of it, and return that.
func (it *hasANext) Next(ctx context.Context) bool {
	if !it.primary.Next(ctx) {
		return false
	}
	it.result = it.qs.QuadDirection(it.primary.Result(), it.dir)
	return true
}

func (it *hasANext) Err() error {
	return it.primary.Err()
}

func (it *hasANext) Result() refs.Ref {
	return it.result
}

// Close the subiterator, the result iterator (if any) and the HasA. It closes
// all subiterators it can, but returns the first error it encounters.
func (it *hasANext) Close() error {
	return it.primary.Close()
}

// A HasA consists of a reference back to the graph.QuadStore that it references,
// a primary subiterator, a direction in which the quads for that subiterator point,
// and a temporary holder for the iterator generated on Contains().
type hasAContains struct {
	qs      QuadIndexer
	primary iterator.Index
	dir     quad.Direction
	results iterator.Scanner
	result  refs.Ref
	err     error
}

// Construct a new HasA iterator, given the quad subiterator, and the quad
// direction for which it stands.
func newHasAContains(qs QuadIndexer, subIt iterator.Index, d quad.Direction) iterator.Index {
	return &hasAContains{
		qs:      qs,
		primary: subIt,
		dir:     d,
	}
}

// Direction accessor.
func (it *hasAContains) Direction() quad.Direction { return it.dir }

// Pass the TagResults down the chain.
func (it *hasAContains) TagResults(dst map[string]refs.Ref) {
	it.primary.TagResults(dst)
}

func (it *hasAContains) String() string {
	return fmt.Sprintf("HasAContains(%v)", it.dir)
}

// Check a value against our internal iterator. In order to do this, we must first open a new
// iterator of "quads that have `val` in our direction", given to us by the quad store,
// and then Next() values out of that iterator and Contains() them against our subiterator.
func (it *hasAContains) Contains(ctx context.Context, val refs.Ref) bool {
	if clog.V(4) {
		clog.Infof("Id is %v", val)
	}
	// TODO(barakmich): Optimize this
	if it.results != nil {
		it.results.Close()
	}
	it.results = it.qs.QuadIterator(it.dir, val).Iterate()
	ok := it.nextContains(ctx)
	if it.err != nil {
		return false
	}
	return ok
}

// nextContains() is shared code between Contains() and GetNextResult() -- calls next on the
// result iterator (a quad iterator based on the last checked value) and returns true if
// another match is made.
func (it *hasAContains) nextContains(ctx context.Context) bool {
	if it.results == nil {
		return false
	}
	for it.results.Next(ctx) {
		link := it.results.Result()
		if clog.V(4) {
			clog.Infof("Quad is %v", it.qs.Quad(link))
		}
		if it.primary.Contains(ctx, link) {
			it.result = it.qs.QuadDirection(link, it.dir)
			return true
		}
	}
	it.err = it.results.Err()
	return false
}

// Get the next result that matches this branch.
func (it *hasAContains) NextPath(ctx context.Context) bool {
	// Order here is important. If the subiterator has a NextPath, then we
	// need do nothing -- there is a next result, and we shouldn't move forward.
	// However, we then need to get the next result from our last Contains().
	//
	// The upshot is, the end of NextPath() bubbles up from the bottom of the
	// iterator tree up, and we need to respect that.
	if clog.V(4) {
		clog.Infof("HASA %p NextPath", it)
	}
	if it.primary.NextPath(ctx) {
		return true
	}
	it.err = it.primary.Err()
	if it.err != nil {
		return false
	}

	result := it.nextContains(ctx) // Sets it.err if there's an error
	if it.err != nil {
		return false
	}
	if clog.V(4) {
		clog.Infof("HASA %p NextPath Returns %v", it, result)
	}
	return result
}

func (it *hasAContains) Err() error {
	return it.err
}

func (it *hasAContains) Result() refs.Ref {
	return it.result
}

// Close the subiterator, the result iterator (if any) and the HasA. It closes
// all subiterators it can, but returns the first error it encounters.
func (it *hasAContains) Close() error {
	err := it.primary.Close()
	if it.results != nil {
		if err2 := it.results.Close(); err2 != nil && err == nil {
			err = err2
		}
	}
	return err
}
