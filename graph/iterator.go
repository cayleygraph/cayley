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
	"reflect"

	"github.com/cayleygraph/cayley/quad"
)

// TODO(barakmich): Linkage is general enough that there are places we take
//the combined arguments `quad.Direction, graph.Value` that it may be worth
//converting these into Linkages. If nothing else, future indexed iterators may
//benefit from the shared representation

// Linkage is a union type representing a set of values established for a given
// quad direction.
type Linkage struct {
	Dir   quad.Direction
	Value Value
}

// TODO(barakmich): Helper functions as needed, eg, ValuesForDirection(quad.Direction) []Value

// Tagger is an interface for iterators that can tag values. Tags are returned as a part of TagResults call.
type Tagger interface {
	Iterator
	Tags() []string
	FixedTags() map[string]Value
	AddTags(tag ...string)
	AddFixedTag(tag string, value Value)
	CopyFromTagger(st Tagger)
}

type Iterator interface {
	// String returns a short textual representation of an iterator.
	String() string

	// Fills a tag-to-result-value map.
	TagResults(map[string]Value)

	// Returns the current result.
	Result() Value

	// Next advances the iterator to the next value, which will then be available through
	// the Result method. It returns false if no further advancement is possible, or if an
	// error was encountered during iteration.  Err should be consulted to distinguish
	// between the two cases.
	Next(ctx context.Context) bool

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

	// Contains returns whether the value is within the set held by the iterator.
	Contains(ctx context.Context, v Value) bool

	// Err returns any error that was encountered by the Iterator.
	Err() error

	// Start iteration from the beginning
	Reset()

	// Create a new iterator just like this one
	Clone() Iterator

	// These methods relate to choosing the right iterator, or optimizing an
	// iterator tree
	//
	// Stats() returns the relative costs of calling the iteration methods for
	// this iterator, as well as the size. Roughly, it will take NextCost * Size
	// "cost units" to get everything out of the iterator. This is a wibbly-wobbly
	// thing, and not exact, but a useful heuristic.
	Stats() IteratorStats

	// Helpful accessor for the number of things in the iterator. The first return
	// value is the size, and the second return value is whether that number is exact,
	// or a conservative estimate.
	Size() (int64, bool)

	// Optimizes an iterator. Can replace the iterator, or merely move things
	// around internally. if it chooses to replace it with a better iterator,
	// returns (the new iterator, true), if not, it returns (self, false).
	Optimize() (Iterator, bool)

	// Return a slice of the subiterators for this iterator.
	SubIterators() []Iterator

	// Close the iterator and do internal cleanup.
	Close() error

	// UID returns the unique identifier of the iterator.
	UID() uint64
}

// DescribeIterator returns a description of the iterator tree.
func DescribeIterator(it Iterator) Description {
	sz, exact := it.Size()
	d := Description{
		UID:  it.UID(),
		Name: it.String(),
		Type: reflect.TypeOf(it).String(),
		Size: sz, Exact: exact,
	}
	if tg, ok := it.(Tagger); ok {
		d.Tags = tg.Tags()
	}
	if sub := it.SubIterators(); len(sub) != 0 {
		d.Iterators = make([]Description, 0, len(sub))
		for _, sit := range sub {
			d.Iterators = append(d.Iterators, DescribeIterator(sit))
		}
	}
	return d
}

type Description struct {
	UID       uint64        `json:",omitempty"`
	Name      string        `json:",omitempty"`
	Type      string        `json:",omitempty"`
	Tags      []string      `json:",omitempty"`
	Size      int64         `json:",omitempty"`
	Exact     bool          `json:",omitempty"`
	Iterators []Description `json:",omitempty"`
}

// ApplyMorphism is a curried function that can generates a new iterator based on some prior iterator.
type ApplyMorphism func(QuadStore, Iterator) Iterator

// Height is a convienence function to measure the height of an iterator tree.
func Height(it Iterator, filter func(Iterator) bool) int {
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

// FixedIterator wraps iterators that are modifiable by addition of fixed value sets.
type FixedIterator interface {
	Iterator
	Add(Value)
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

type StatsContainer struct {
	UID  uint64
	Type string
	IteratorStats
	SubIts []StatsContainer
}

func DumpStats(it Iterator) StatsContainer {
	var out StatsContainer
	out.IteratorStats = it.Stats()
	out.Type = reflect.TypeOf(it).String()
	out.UID = it.UID()
	for _, sub := range it.SubIterators() {
		out.SubIts = append(out.SubIts, DumpStats(sub))
	}
	return out
}
