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
	"strings"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/quad"
)

type Tagger struct {
	tags      []string
	fixedTags map[string]Value
}

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

// Add a tag to the iterator.
func (t *Tagger) Add(tag ...string) {
	t.tags = append(t.tags, tag...)
}

func (t *Tagger) AddFixed(tag string, value Value) {
	if t.fixedTags == nil {
		t.fixedTags = make(map[string]Value)
	}
	t.fixedTags[tag] = value
}

// Tags returns the tags held in the tagger. The returned value must not be mutated.
func (t *Tagger) Tags() []string {
	return t.tags
}

// Fixed returns the fixed tags held in the tagger. The returned value must not be mutated.
func (t *Tagger) Fixed() map[string]Value {
	return t.fixedTags
}

func (t *Tagger) TagResult(dst map[string]Value, v Value) {
	for _, tag := range t.Tags() {
		dst[tag] = v
	}

	for tag, value := range t.Fixed() {
		dst[tag] = value
	}
}

func (t *Tagger) CopyFrom(src Iterator) {
	t.CopyFromTagger(src.Tagger())
}

func (t *Tagger) CopyFromTagger(st *Tagger) {
	t.tags = append(t.tags, st.tags...)

	if t.fixedTags == nil {
		t.fixedTags = make(map[string]Value, len(st.fixedTags))
	}
	for k, v := range st.fixedTags {
		t.fixedTags[k] = v
	}
}

type Iterator interface {
	// String returns a short textual representation of an iterator.
	String() string

	Tagger() *Tagger

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

	// Returns a string relating to what the function of the iterator is. By
	// knowing the names of the iterators, we can devise optimization strategies.
	Type() Type

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
		Type: it.Type(),
		Tags: it.Tagger().Tags(),
		Size: sz, Exact: exact,
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
	Type      Type          `json:",omitempty"`
	Tags      []string      `json:",omitempty"`
	Size      int64         `json:",omitempty"`
	Exact     bool          `json:",omitempty"`
	Iterators []Description `json:",omitempty"`
}

// ApplyMorphism is a curried function that can generates a new iterator based on some prior iterator.
type ApplyMorphism func(QuadStore, Iterator) Iterator

// CanNext is a helper for checking if iterator can be Next()'ed.
func CanNext(it Iterator) bool {
	_, ok := it.(NoNext)
	return !ok
}

// NoNext is an optional interface to signal that iterator should be Contain()'ed instead of Next()'ing if possible.
type NoNext interface {
	NoNext()
}

// Height is a convienence function to measure the height of an iterator tree.
func Height(it Iterator, until Type) int {
	if it.Type() == until {
		return 1
	}
	subs := it.SubIterators()
	maxDepth := 0
	for _, sub := range subs {
		h := Height(sub, until)
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

// Type enumerates the set of Iterator types.
type Type string

// These are the iterator types, defined as constants
const (
	Invalid     = Type("")
	All         = Type("all")
	And         = Type("and")
	Or          = Type("or")
	HasA        = Type("hasa")
	LinksTo     = Type("linksto")
	Comparison  = Type("comparison")
	Null        = Type("null")
	Err         = Type("error")
	Fixed       = Type("fixed")
	Not         = Type("not")
	Optional    = Type("optional")
	Materialize = Type("materialize")
	Unique      = Type("unique")
	Limit       = Type("limit")
	Skip        = Type("skip")
	Regex       = Type("regexp")
	Count       = Type("count")
	Recursive   = Type("recursive")
	Resolver    = Type("resolver")
)

// String returns a string representation of the Type.
func (t Type) String() string {
	if t == "" {
		return "illegal-type"
	}
	return string(t)
}

type StatsContainer struct {
	UID  uint64
	Type Type
	IteratorStats
	SubIts []StatsContainer
}

func DumpStats(it Iterator) StatsContainer {
	var out StatsContainer
	out.IteratorStats = it.Stats()
	out.Type = it.Type()
	out.UID = it.UID()
	for _, sub := range it.SubIterators() {
		out.SubIts = append(out.SubIts, DumpStats(sub))
	}
	return out
}

// Utility logging functions for when an iterator gets called Next upon, or Contains upon, as
// well as what they return. Highly useful for tracing the execution path of a query.

func ContainsLogIn(it Iterator, val Value) {
	if clog.V(4) {
		clog.Infof("%s %d CHECK CONTAINS %v", strings.ToUpper(it.Type().String()), it.UID(), val)
	}
}

func ContainsLogOut(it Iterator, val Value, good bool) bool {
	if clog.V(4) {
		if good {
			clog.Infof("%s %d CHECK CONTAINS %v GOOD", strings.ToUpper(it.Type().String()), it.UID(), val)
		} else {
			clog.Infof("%s %d CHECK CONTAINS %v BAD", strings.ToUpper(it.Type().String()), it.UID(), val)
		}
	}
	return good
}

func NextLogIn(it Iterator) {
	if clog.V(4) {
		clog.Infof("%s %d NEXT", strings.ToUpper(it.Type().String()), it.UID())
	}
}

func NextLogOut(it Iterator, ok bool) bool {
	if clog.V(4) {
		if ok {
			val := it.Result()
			clog.Infof("%s %d NEXT IS %v (%T)", strings.ToUpper(it.Type().String()), it.UID(), val, val)
		} else {
			clog.Infof("%s %d NEXT DONE", strings.ToUpper(it.Type().String()), it.UID())
		}
	}
	return ok
}
