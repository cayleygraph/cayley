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

// Define the general iterator interface.

import (
	"context"
	"fmt"

	"github.com/cayleygraph/cayley/graph/refs"
)

var (
	_ Shape = &Null{}
	_ Shape = &Error{}
)

// TaggerBase is a base interface for Tagger and TaggerShape.
type TaggerBase interface {
	Tags() []string
	FixedTags() map[string]refs.Ref
	AddTags(tag ...string)
	AddFixedTag(tag string, value refs.Ref)
}

// Base is a set of common methods for Scanner and Index iterators.
type Base interface {
	// String returns a short textual representation of an iterator.
	String() string

	// Fills a tag-to-result-value map.
	TagResults(map[string]refs.Ref)

	// Returns the current result.
	Result() refs.Ref

	// These methods are the heart and soul of the iterator, as they constitute
	// the iteration interface.
	//
	// To get the full results of iteration, do the following:
	//
	//  for it.Next(ctx) {
	//  	val := it.Result()
	//  	... do things with val.
	//  	for it.NextPath(ctx) {
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
	Base

	// Next advances the iterator to the next value, which will then be available through
	// the Result method. It returns false if no further advancement is possible, or if an
	// error was encountered during iteration.  Err should be consulted to distinguish
	// between the two cases.
	Next(ctx context.Context) bool
}

// Index is an index lookup iterator. It allows to check if an index contains a specific value.
type Index interface {
	Base

	// Contains returns whether the value is within the set held by the iterator.
	//
	// It will set Result to the matching subtree. TagResults can be used to collect values from tree branches.
	Contains(ctx context.Context, v refs.Ref) bool
}

// TaggerShape is an interface for iterators that can tag values. Tags are returned as a part of TagResults call.
type TaggerShape interface {
	Shape
	TaggerBase
	CopyFromTagger(st TaggerBase)
}

type Costs struct {
	ContainsCost int64
	NextCost     int64
	Size         refs.Size
}

// Shape is an iterator shape, similar to a query plan. But the plan is not specific in this
// case - it is used to reorder query branches, and the decide what branches will be scanned
// and what branches will lookup values (hopefully from the index, but not necessarily).
type Shape interface {
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
	Stats(ctx context.Context) (Costs, error)

	// Optimizes an iterator. Can replace the iterator, or merely move things
	// around internally. if it chooses to replace it with a better iterator,
	// returns (the new iterator, true), if not, it returns (self, false).
	Optimize(ctx context.Context) (Shape, bool)

	// Return a slice of the subiterators for this iterator.
	SubIterators() []Shape
}

type Morphism func(Shape) Shape

func IsNull(it Shape) bool {
	if _, ok := it.(*Null); ok {
		return true
	}
	return false
}

// Height is a convienence function to measure the height of an iterator tree.
func Height(it Shape, filter func(Shape) bool) int {
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

// Null is the simplest iterator -- the Null iterator. It contains nothing.
// It is the empty set. Often times, queries that contain one of these match nothing,
// so it's important to give it a special iterator.
type Null struct{}

// NewNull creates a new Null iterator
// Fairly useless New function.
func NewNull() *Null {
	return &Null{}
}

// Iterate implements Iterator
func (it *Null) Iterate() Scanner {
	return it
}

// Lookup implements Iterator
func (it *Null) Lookup() Index {
	return it
}

// Fill the map based on the tags assigned to this iterator.
func (it *Null) TagResults(dst map[string]refs.Ref) {}

func (it *Null) Contains(ctx context.Context, v refs.Ref) bool {
	return false
}

// A good iterator will close itself when it returns true.
// Null has nothing it needs to do.
func (it *Null) Optimize(ctx context.Context) (Shape, bool) { return it, false }

func (it *Null) String() string {
	return "Null"
}

func (it *Null) Next(ctx context.Context) bool {
	return false
}

func (it *Null) Err() error {
	return nil
}

func (it *Null) Result() refs.Ref {
	return nil
}

func (it *Null) SubIterators() []Shape {
	return nil
}

func (it *Null) NextPath(ctx context.Context) bool {
	return false
}

func (it *Null) Reset() {}

func (it *Null) Close() error {
	return nil
}

// A null iterator costs nothing. Use it!
func (it *Null) Stats(ctx context.Context) (Costs, error) {
	return Costs{}, nil
}

// Error iterator always returns a single error with no other results.
type Error struct {
	err error
}

func NewError(err error) *Error {
	return &Error{err: err}
}

func (it *Error) Iterate() Scanner {
	return it
}

func (it *Error) Lookup() Index {
	return it
}

// Fill the map based on the tags assigned to this iterator.
func (it *Error) TagResults(dst map[string]refs.Ref) {}

func (it *Error) Contains(ctx context.Context, v refs.Ref) bool {
	return false
}

func (it *Error) Optimize(ctx context.Context) (Shape, bool) { return it, false }

func (it *Error) String() string {
	return fmt.Sprintf("Error(%v)", it.err)
}

func (it *Error) Next(ctx context.Context) bool {
	return false
}

func (it *Error) Err() error {
	return it.err
}

func (it *Error) Result() refs.Ref {
	return nil
}

func (it *Error) SubIterators() []Shape {
	return nil
}

func (it *Error) NextPath(ctx context.Context) bool {
	return false
}

func (it *Error) Reset() {}

func (it *Error) Close() error {
	return it.err
}

func (it *Error) Stats(ctx context.Context) (Costs, error) {
	return Costs{}, nil
}
