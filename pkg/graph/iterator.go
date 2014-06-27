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

// Define the general iterator interface, as well as the BaseIterator which all
// iterators can "inherit" from to get default iterator functionality.

import (
	"container/list"
	"fmt"
	"github.com/barakmich/glog"
	"strings"
)

var iterator_n int = 0

type Iterator interface {
	// Tags are the way we handle results. By adding a tag to an iterator, we can
	// "name" it, in a sense, and at each step of iteration, get a named result.
	// TagResults() is therefore the handy way of walking an iterator tree and
	// getting the named results.
	//
	// Tag Accessors.
	AddTag(string)
	Tags() []string
	AddFixedTag(string, TSVal)
	FixedTags() map[string]TSVal
	CopyTagsFrom(Iterator)
	// Fills a tag-to-result-value map.
	TagResults(*map[string]TSVal)
	// Returns the current result.
	LastResult() TSVal
	// DEPRECATED -- Fills a ResultTree struct with Result().
	GetResultTree() *ResultTree

	// These methods are the heart and soul of the iterator, as they constitute
	// the iteration interface.
	//
	// To get the full results of iteraton, do the following:
	// while (!Next()):
	//   emit result
	//   while (!NextResult()):
	//       emit result
	//
	// All of them should set iterator.Last to be the last returned value, to
	// make results work.
	//
	// Next() advances the iterator and returns the next valid result. Returns
	// (<value>, true) or (nil, false)
	Next() (TSVal, bool)
	// NextResult() advances iterators that may have more than one valid result,
	// from the bottom up.
	NextResult() bool
	// Check(), given a value, returns whether or not that value is within the set
	// held by this iterator.
	Check(TSVal) bool
	// Start iteration from the beginning
	Reset()
	// Create a new iterator just like this one
	Clone() Iterator
	// These methods relate to choosing the right iterator, or optimizing an
	// iterator tree
	//
	// GetStats() returns the relative costs of calling the iteration methods for
	// this iterator, as well as the size. Roughly, it will take NextCost * Size
	// "cost units" to get everything out of the iterator. This is a wibbly-wobbly
	// thing, and not exact, but a useful heuristic.
	GetStats() *IteratorStats
	// Helpful accessor for the number of things in the iterator. The first return
	// value is the size, and the second return value is whether that number is exact,
	// or a conservative estimate.
	Size() (int64, bool)
	// Returns a string relating to what the function of the iterator is. By
	// knowing the names of the iterators, we can devise optimization strategies.
	Type() string
	// Optimizes an iterator. Can replace the iterator, or merely move things
	// around internally. if it chooses to replace it with a better iterator,
	// returns (the new iterator, true), if not, it returns (self, false).
	Optimize() (Iterator, bool)
	// Return a list of the subiterators for this iterator.
	GetSubIterators() *list.List

	// Return a string representation of the iterator, indented by the given amount.
	DebugString(int) string
	// Return whether this iterator is relaiably nextable. Most iterators are.
	// However, some iterators, like "not" are, by definition, the whole database
	// except themselves. Next() on these is unproductive, if impossible.
	Nextable() bool
	// Close the iterator and do internal cleanup.
	Close()
	GetUid() int
}

type IteratorStats struct {
	CheckCost int64
	NextCost  int64
	Size      int64
}

// The Base iterator is the iterator other iterators inherit from to get some
// default functionality.
type BaseIterator struct {
	Last      TSVal
	tags      []string
	fixedTags map[string]TSVal
	nextable  bool
	uid       int
}

// Called by subclases.
func BaseIteratorInit(b *BaseIterator) {
	// Your basic iterator is nextable
	b.nextable = true
	b.uid = iterator_n
	if glog.V(2) {
		iterator_n++
	}
}

func (b *BaseIterator) GetUid() int {
	return b.uid
}

// Adds a tag to the iterator. Most iterators don't need to override.
func (b *BaseIterator) AddTag(tag string) {
	if b.tags == nil {
		b.tags = make([]string, 0)
	}
	b.tags = append(b.tags, tag)
}

func (b *BaseIterator) AddFixedTag(tag string, value TSVal) {
	if b.fixedTags == nil {
		b.fixedTags = make(map[string]TSVal)
	}
	b.fixedTags[tag] = value
}

// Returns the tags.
func (b *BaseIterator) Tags() []string {
	return b.tags
}

func (b *BaseIterator) FixedTags() map[string]TSVal {
	return b.fixedTags
}

func (b *BaseIterator) CopyTagsFrom(other_it Iterator) {
	for _, tag := range other_it.Tags() {
		b.AddTag(tag)
	}

	for k, v := range other_it.FixedTags() {
		b.AddFixedTag(k, v)
	}

}

// Prints a silly debug string. Most classes override.
func (n *BaseIterator) DebugString(indent int) string {
	return fmt.Sprintf("%s(base)", strings.Repeat(" ", indent))
}

// Nothing in a base iterator.
func (n *BaseIterator) Check(v TSVal) bool {
	return false
}

// Base iterators should never appear in a tree if they are, select against
// them.
func (n *BaseIterator) GetStats() *IteratorStats {
	return &IteratorStats{100000, 100000, 100000}
}

// DEPRECATED
func (b *BaseIterator) GetResultTree() *ResultTree {
	tree := NewResultTree(b.LastResult())
	return tree
}

// Nothing in a base iterator.
func (n *BaseIterator) Next() (TSVal, bool) {
	return nil, false
}

func (n *BaseIterator) NextResult() bool {
	return false
}

// Returns the last result of an iterator.
func (n *BaseIterator) LastResult() TSVal {
	return n.Last
}

// If you're empty and you know it, clap your hands.
func (n *BaseIterator) Size() (int64, bool) {
	return 0, true
}

// No subiterators. Only those with subiterators need to do anything here.
func (n *BaseIterator) GetSubIterators() *list.List {
	return nil
}

// Accessor
func (b *BaseIterator) Nextable() bool { return b.nextable }

// Fill the map based on the tags assigned to this iterator. Default
// functionality works well for most iterators.
func (a *BaseIterator) TagResults(out_map *map[string]TSVal) {
	for _, tag := range a.Tags() {
		(*out_map)[tag] = a.LastResult()
	}

	for tag, value := range a.FixedTags() {
		(*out_map)[tag] = value
	}
}

// Nothing to clean up.
//func (a *BaseIterator) Close() {}
func (a *NullIterator) Close() {}

func (a *BaseIterator) Reset() {}

// Here we define the simplest base iterator -- the Null iterator. It contains nothing.
// It is the empty set. Often times, queries that contain one of these match nothing,
// so it's important to give it a special iterator.
type NullIterator struct {
	BaseIterator
}

// Fairly useless New function.
func NewNullIterator() *NullIterator {
	var n NullIterator
	return &n
}

func (n *NullIterator) Clone() Iterator { return NewNullIterator() }

// Name the null iterator.
func (n *NullIterator) Type() string { return "null" }

// A good iterator will close itself when it returns true.
// Null has nothing it needs to do.
func (n *NullIterator) Optimize() (Iterator, bool) { return n, false }

// Print the null iterator.
func (n *NullIterator) DebugString(indent int) string {
	return strings.Repeat(" ", indent) + "(null)"
}

// A null iterator costs nothing. Use it!
func (n *NullIterator) GetStats() *IteratorStats {
	return &IteratorStats{0, 0, 0}
}

// Utility logging functions for when an iterator gets called Next upon, or Check upon, as
// well as what they return. Highly useful for tracing the execution path of a query.
func CheckLogIn(it Iterator, val TSVal) {
	if glog.V(4) {
		glog.V(4).Infof("%s %d CHECK %d", strings.ToUpper(it.Type()), it.GetUid(), val)
	}
}

func CheckLogOut(it Iterator, val TSVal, good bool) bool {
	if glog.V(4) {
		if good {
			glog.V(4).Infof("%s %d CHECK %d GOOD", strings.ToUpper(it.Type()), it.GetUid(), val)
		} else {
			glog.V(4).Infof("%s %d CHECK %d BAD", strings.ToUpper(it.Type()), it.GetUid(), val)
		}
	}
	return good
}

func NextLogIn(it Iterator) {
	if glog.V(4) {
		glog.V(4).Infof("%s %d NEXT", strings.ToUpper(it.Type()), it.GetUid())
	}
}

func NextLogOut(it Iterator, val TSVal, ok bool) (TSVal, bool) {
	if glog.V(4) {
		if ok {
			glog.V(4).Infof("%s %d NEXT IS %d", strings.ToUpper(it.Type()), it.GetUid(), val)
		} else {
			glog.V(4).Infof("%s %d NEXT DONE", strings.ToUpper(it.Type()), it.GetUid())
		}
	}
	return val, ok
}
