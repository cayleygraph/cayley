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

// Define the general iterator interface, as well as the Base which all
// iterators can "inherit" from to get default iterator functionality.

import (
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/barakmich/glog"

	"github.com/google/cayley/graph"
)

var nextIteratorID uintptr

func nextID() uintptr {
	return atomic.AddUintptr(&nextIteratorID, 1) - 1
}

// The Base iterator is the iterator other iterators inherit from to get some
// default functionality.
type Base struct {
	Last      graph.TSVal
	tags      []string
	fixedTags map[string]graph.TSVal
	canNext   bool
	uid       uintptr
}

// Called by subclases.
func BaseInit(it *Base) {
	// Your basic iterator is nextable
	it.canNext = true
	if glog.V(2) {
		it.uid = nextID()
	}
}

func (it *Base) UID() uintptr {
	return it.uid
}

// Adds a tag to the iterator. Most iterators don't need to override.
func (it *Base) AddTag(tag string) {
	if it.tags == nil {
		it.tags = make([]string, 0)
	}
	it.tags = append(it.tags, tag)
}

func (it *Base) AddFixedTag(tag string, value graph.TSVal) {
	if it.fixedTags == nil {
		it.fixedTags = make(map[string]graph.TSVal)
	}
	it.fixedTags[tag] = value
}

// Returns the tags.
func (it *Base) Tags() []string {
	return it.tags
}

func (it *Base) FixedTags() map[string]graph.TSVal {
	return it.fixedTags
}

func (it *Base) CopyTagsFrom(other_it graph.Iterator) {
	for _, tag := range other_it.Tags() {
		it.AddTag(tag)
	}

	for k, v := range other_it.FixedTags() {
		it.AddFixedTag(k, v)
	}

}

// Prints a silly debug string. Most classes override.
func (it *Base) DebugString(indent int) string {
	return fmt.Sprintf("%s(base)", strings.Repeat(" ", indent))
}

// Nothing in a base iterator.
func (it *Base) Check(v graph.TSVal) bool {
	return false
}

// Base iterators should never appear in a tree if they are, select against
// them.
func (it *Base) Stats() graph.IteratorStats {
	return graph.IteratorStats{100000, 100000, 100000}
}

// DEPRECATED
func (it *Base) ResultTree() *graph.ResultTree {
	tree := graph.NewResultTree(it.LastResult())
	return tree
}

// Nothing in a base iterator.
func (it *Base) Next() (graph.TSVal, bool) {
	return nil, false
}

func (it *Base) NextResult() bool {
	return false
}

// Returns the last result of an iterator.
func (it *Base) LastResult() graph.TSVal {
	return it.Last
}

// If you're empty and you know it, clap your hands.
func (it *Base) Size() (int64, bool) {
	return 0, true
}

// No subiterators. Only those with subiterators need to do anything here.
func (it *Base) SubIterators() []graph.Iterator {
	return nil
}

// Accessor
func (it *Base) CanNext() bool { return it.canNext }

// Fill the map based on the tags assigned to this iterator. Default
// functionality works well for most iterators.
func (it *Base) TagResults(out_map *map[string]graph.TSVal) {
	for _, tag := range it.Tags() {
		(*out_map)[tag] = it.LastResult()
	}

	for tag, value := range it.FixedTags() {
		(*out_map)[tag] = value
	}
}

// Nothing to clean up.
// func (it *Base) Close() {}

func (it *Null) Close() {}

func (it *Base) Reset() {}

// Here we define the simplest base iterator -- the Null iterator. It contains nothing.
// It is the empty set. Often times, queries that contain one of these match nothing,
// so it's important to give it a special iterator.
type Null struct {
	Base
}

// Fairly useless New function.
func NewNull() *Null {
	return &Null{}
}

func (it *Null) Clone() graph.Iterator { return NewNull() }

// Name the null iterator.
func (it *Null) Type() string { return "null" }

// A good iterator will close itself when it returns true.
// Null has nothing it needs to do.
func (it *Null) Optimize() (graph.Iterator, bool) { return it, false }

// Print the null iterator.
func (it *Null) DebugString(indent int) string {
	return strings.Repeat(" ", indent) + "(null)"
}

// A null iterator costs nothing. Use it!
func (it *Null) Stats() graph.IteratorStats {
	return graph.IteratorStats{}
}
