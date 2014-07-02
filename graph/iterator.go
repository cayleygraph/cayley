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
	"strings"

	"github.com/barakmich/glog"
)

type Iterator interface {
	// Tags are the way we handle results. By adding a tag to an iterator, we can
	// "name" it, in a sense, and at each step of iteration, get a named result.
	// TagResults() is therefore the handy way of walking an iterator tree and
	// getting the named results.
	//
	// Tag Accessors.
	AddTag(string)
	Tags() []string
	AddFixedTag(string, Value)
	FixedTags() map[string]Value
	CopyTagsFrom(Iterator)

	// Fills a tag-to-result-value map.
	TagResults(map[string]Value)

	// Returns the current result.
	Result() Value

	// DEPRECATED -- Fills a ResultTree struct with Result().
	ResultTree() *ResultTree

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
	Next() (Value, bool)

	// NextResult() advances iterators that may have more than one valid result,
	// from the bottom up.
	NextResult() bool

	// Return whether this iterator is reliably nextable. Most iterators are.
	// However, some iterators, like "not" are, by definition, the whole database
	// except themselves. Next() on these is unproductive, if impossible.
	CanNext() bool

	// Check(), given a value, returns whether or not that value is within the set
	// held by this iterator.
	Check(Value) bool

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
	Type() string

	// Optimizes an iterator. Can replace the iterator, or merely move things
	// around internally. if it chooses to replace it with a better iterator,
	// returns (the new iterator, true), if not, it returns (self, false).
	Optimize() (Iterator, bool)

	// Return a slice of the subiterators for this iterator.
	SubIterators() []Iterator

	// Return a string representation of the iterator, indented by the given amount.
	DebugString(int) string

	// Close the iterator and do internal cleanup.
	Close()

	UID() uintptr
}

type FixedIterator interface {
	Iterator
	Add(Value)
}

type IteratorStats struct {
	CheckCost int64
	NextCost  int64
	Size      int64
}

// Utility logging functions for when an iterator gets called Next upon, or Check upon, as
// well as what they return. Highly useful for tracing the execution path of a query.
func CheckLogIn(it Iterator, val Value) {
	if glog.V(4) {
		glog.V(4).Infof("%s %d CHECK %d", strings.ToUpper(it.Type()), it.UID(), val)
	}
}

func CheckLogOut(it Iterator, val Value, good bool) bool {
	if glog.V(4) {
		if good {
			glog.V(4).Infof("%s %d CHECK %d GOOD", strings.ToUpper(it.Type()), it.UID(), val)
		} else {
			glog.V(4).Infof("%s %d CHECK %d BAD", strings.ToUpper(it.Type()), it.UID(), val)
		}
	}
	return good
}

func NextLogIn(it Iterator) {
	if glog.V(4) {
		glog.V(4).Infof("%s %d NEXT", strings.ToUpper(it.Type()), it.UID())
	}
}

func NextLogOut(it Iterator, val Value, ok bool) (Value, bool) {
	if glog.V(4) {
		if ok {
			glog.V(4).Infof("%s %d NEXT IS %d", strings.ToUpper(it.Type()), it.UID(), val)
		} else {
			glog.V(4).Infof("%s %d NEXT DONE", strings.ToUpper(it.Type()), it.UID())
		}
	}
	return val, ok
}
