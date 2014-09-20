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
	"fmt"
	"strings"
	"sync"

	"github.com/barakmich/glog"
	"github.com/google/cayley/quad"
)

type Tagger struct {
	tags      []string
	fixedTags map[string]Value
}

// Adds a tag to the iterator.
func (t *Tagger) Add(tag string) {
	t.tags = append(t.tags, tag)
}

func (t *Tagger) AddFixed(tag string, value Value) {
	if t.fixedTags == nil {
		t.fixedTags = make(map[string]Value)
	}
	t.fixedTags[tag] = value
}

// Returns the tags. The returned value must not be mutated.
func (t *Tagger) Tags() []string {
	return t.tags
}

// Returns the fixed tags. The returned value must not be mutated.
func (t *Tagger) Fixed() map[string]Value {
	return t.fixedTags
}

func (t *Tagger) CopyFrom(src Iterator) {
	st := src.Tagger()

	t.tags = append(t.tags, st.tags...)

	if t.fixedTags == nil {
		t.fixedTags = make(map[string]Value, len(st.fixedTags))
	}
	for k, v := range st.fixedTags {
		t.fixedTags[k] = v
	}
}

type Iterator interface {
	Tagger() *Tagger

	// Fills a tag-to-result-value map.
	TagResults(map[string]Value)

	// Returns the current result.
	Result() Value

	// DEPRECATED -- Fills a ResultTree struct with Result().
	ResultTree() *ResultTree

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
	// All of them should set iterator.Last to be the last returned value, to
	// make results work.
	//
	// NextPath() advances iterators that may have more than one valid result,
	// from the bottom up.
	NextPath() bool

	// Contains returns whether the value is within the set held by the iterator.
	Contains(Value) bool

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

	// Return a string representation of the iterator.
	Describe() Description

	// Close the iterator and do internal cleanup.
	Close()

	// UID returns the unique identifier of the iterator.
	UID() uint64
}

type Description struct {
	UID       uint64         `json:",omitempty"`
	Name      string         `json:",omitempty"`
	Type      Type           `json:",omitempty"`
	Tags      []string       `json:",omitempty"`
	Size      int64          `json:",omitempty"`
	Direction quad.Direction `json:",omitempty"`
	Iterator  *Description   `json:",omitempty"`
	Iterators []Description  `json:",omitempty"`
}

type Nexter interface {
	// Next advances the iterator to the next value, which will then be available through
	// the Result method. It returns false if no further advancement is possible.
	Next() bool

	Iterator
}

// Next is a convenience function that conditionally calls the Next method
// of an Iterator if it is a Nexter. If the Iterator is not a Nexter, Next
// returns false.
func Next(it Iterator) bool {
	if n, ok := it.(Nexter); ok {
		return n.Next()
	}
	glog.Errorln("Nexting an un-nextable iterator")
	return false
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
	Next         int64
	Contains     int64
	ContainsNext int64
}

// Type enumerates the set of Iterator types.
type Type int

const (
	Invalid Type = iota
	All
	And
	Or
	HasA
	LinksTo
	Comparison
	Null
	Fixed
	Not
	Optional
	Materialize
)

var (
	// We use a sync.Mutex rather than an RWMutex since the client packages keep
	// the Type that was returned, so the only possibility for contention is at
	// initialization.
	lock sync.Mutex
	// These strings must be kept in order consistent with the Type const block above.
	types = []string{
		"invalid",
		"all",
		"and",
		"or",
		"hasa",
		"linksto",
		"comparison",
		"null",
		"fixed",
		"not",
		"optional",
		"materialize",
	}
)

// RegisterIterator adds a new iterator type to the set of acceptable types, returning
// the registered Type.
// Calls to Register are idempotent and must be made prior to use of the iterator.
// The conventional approach for use is to include a call to Register in a package
// init() function, saving the Type to a private package var.
func RegisterIterator(name string) Type {
	lock.Lock()
	defer lock.Unlock()
	for i, t := range types {
		if t == name {
			return Type(i)
		}
	}
	types = append(types, name)
	return Type(len(types) - 1)
}

// String returns a string representation of the Type.
func (t Type) String() string {
	if t < 0 || int(t) >= len(types) {
		return "illegal-type"
	}
	return types[t]
}

func (t *Type) MarshalText() (text []byte, err error) {
	if *t < 0 || int(*t) >= len(types) {
		return nil, fmt.Errorf("graph: illegal iterator type: %d", *t)
	}
	return []byte(types[*t]), nil
}

func (t *Type) UnmarshalText(text []byte) error {
	s := string(text)
	for i, c := range types[1:] {
		if c == s {
			*t = Type(i + 1)
			return nil
		}
	}
	return fmt.Errorf("graph: unknown iterator label: %q", text)
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
	if glog.V(4) {
		glog.V(4).Infof("%s %d CHECK CONTAINS %d", strings.ToUpper(it.Type().String()), it.UID(), val)
	}
}

func ContainsLogOut(it Iterator, val Value, good bool) bool {
	if glog.V(4) {
		if good {
			glog.V(4).Infof("%s %d CHECK CONTAINS %d GOOD", strings.ToUpper(it.Type().String()), it.UID(), val)
		} else {
			glog.V(4).Infof("%s %d CHECK CONTAINS %d BAD", strings.ToUpper(it.Type().String()), it.UID(), val)
		}
	}
	return good
}

func NextLogIn(it Iterator) {
	if glog.V(4) {
		glog.V(4).Infof("%s %d NEXT", strings.ToUpper(it.Type().String()), it.UID())
	}
}

func NextLogOut(it Iterator, val Value, ok bool) bool {
	if glog.V(4) {
		if ok {
			glog.V(4).Infof("%s %d NEXT IS %d", strings.ToUpper(it.Type().String()), it.UID(), val)
		} else {
			glog.V(4).Infof("%s %d NEXT DONE", strings.ToUpper(it.Type().String()), it.UID())
		}
	}
	return ok
}
