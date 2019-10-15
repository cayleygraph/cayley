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

// Define the general iterator interface.

import (
	"context"

	"github.com/cayleygraph/quad"
)

// TODO(barakmich): Linkage is general enough that there are places we take
//the combined arguments `quad.Direction, graph.Ref` that it may be worth
//converting these into Linkages. If nothing else, future indexed iterators may
//benefit from the shared representation

// Linkage is a union type representing a set of values established for a given
// quad direction.
type Linkage struct {
	Dir   quad.Direction
	Value Ref
}

// TODO(barakmich): Helper functions as needed, eg, ValuesForDirection(quad.Direction) []Ref

// TaggerBase is a base interface for Tagger and TaggerShape.
type TaggerBase interface {
	Tags() []string
	FixedTags() map[string]Ref
	AddTags(tag ...string)
	AddFixedTag(tag string, value Ref)
}

// IteratorBase is a set of common methods for Scanner and Index iterators.
type IteratorBase interface {
	// String returns a short textual representation of an iterator.
	String() string

	// Fills a tag-to-result-value map.
	TagResults(map[string]Ref)

	// Returns the current result.
	Result() Ref

	// These methods are the heart and soul of the iterator, as they constitute
	// the iteration interface.
	//
	// To get the full results of iteration, do the following:
	//
	//  for graph.Next(it) {
	//  	val := it.Result()
	//  	... do things with val.
	//  	for it.NextPath() {
	//  		... find other paths to iterate
	//  	}
	//  }
	//
	// All of them should set iterator.result to be the last returned value, to
	// make results work.
	//
	// NextPath() advances iterators that may have more than one valid result,
	// from the bottom up.
	NextPath(ctx context.Context) bool

	// Err returns any error that was encountered by the Iterator.
	Err() error

	// TODO: make a requirement that Err should return ErrClosed after Close is called

	// Close the iterator and do internal cleanup.
	Close() error
}

// Scanner is an iterator that lists all results sequentially, but not necessarily in a sorted order.
type Scanner interface {
	IteratorBase

	// Next advances the iterator to the next value, which will then be available through
	// the Result method. It returns false if no further advancement is possible, or if an
	// error was encountered during iteration.  Err should be consulted to distinguish
	// between the two cases.
	Next(ctx context.Context) bool
}

// Index is an index lookup iterator. It allows to check if an index contains a specific value.
type Index interface {
	IteratorBase

	// Contains returns whether the value is within the set held by the iterator.
	//
	// It will set Result to the matching subtree. TagResults can be used to collect values from tree branches.
	Contains(ctx context.Context, v Ref) bool
}

// Tagger is an interface for iterators that can tag values. Tags are returned as a part of TagResults call.
type TaggerShape interface {
	IteratorShape
	TaggerBase
	CopyFromTagger(st TaggerBase)
}

type IteratorCosts struct {
	ContainsCost int64
	NextCost     int64
	Size         Size
}

// Shape is an iterator shape, similar to a query plan. But the plan is not specific in this
// case - it is used to reorder query branches, and the decide what branches will be scanned
// and what branches will lookup values (hopefully from the index, but not necessarily).
type IteratorShape interface {
	// TODO(dennwc): merge with shape.Shape

	// String returns a short textual representation of an iterator.
	String() string

	// Iterate starts this iterator in scanning mode. Resulting iterator will list all
	// results sequentially, but not necessary in the sorted order. Caller must close
	// the iterator.
	Iterate() Scanner

	// Lookup starts this iterator in an index lookup mode. Depending on the iterator type,
	// this may still involve database scans. Resulting iterator allows to check an index
	// contains a specified value. Caller must close the iterator.
	Lookup() Index

	// These methods relate to choosing the right iterator, or optimizing an
	// iterator tree
	//
	// Stats() returns the relative costs of calling the iteration methods for
	// this iterator, as well as the size. Roughly, it will take NextCost * Size
	// "cost units" to get everything out of the iterator. This is a wibbly-wobbly
	// thing, and not exact, but a useful heuristic.
	Stats(ctx context.Context) (IteratorCosts, error)

	// Optimizes an iterator. Can replace the iterator, or merely move things
	// around internally. if it chooses to replace it with a better iterator,
	// returns (the new iterator, true), if not, it returns (self, false).
	Optimize(ctx context.Context) (IteratorShape, bool)

	// Return a slice of the subiterators for this iterator.
	SubIterators() []IteratorShape
}

// ApplyMorphism is a curried function that can generates a new iterator based on some prior iterator.
type ApplyMorphism func(QuadStore, IteratorShape) IteratorShape

// Height is a convienence function to measure the height of an iterator tree.
func Height(it IteratorShape, filter func(IteratorShape) bool) int {
	if filter != nil && !filter(it) {
		return 1
	}
	subs := it.SubIterators()
	maxDepth := 0
	for _, sub := range subs {
		h := Height(sub, filter)
		if h > maxDepth {
			maxDepth = h
		}
	}
	return maxDepth + 1
}

type IteratorStats struct {
	ContainsCost int64
	NextCost     int64
	Size         int64
	ExactSize    bool
	Next         int64
	Contains     int64
	ContainsNext int64
}
