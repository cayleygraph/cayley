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
	"strings"
	"sync/atomic"

	"github.com/google/cayley/graph"
)

var nextIteratorID uint64

func NextUID() uint64 {
	return atomic.AddUint64(&nextIteratorID, 1) - 1
}

// The Base iterator is the iterator other iterators inherit from to get some
// default functionality.
type Base struct {
	canNext bool
}

// Called by subclases.
func BaseInit(it *Base) {
	// Your basic iterator is nextable
	it.canNext = true
}

func (it *Base) NextResult() bool {
	return false
}

// Accessor
func (it *Base) CanNext() bool { return it.canNext }

// Here we define the simplest iterator -- the Null iterator. It contains nothing.
// It is the empty set. Often times, queries that contain one of these match nothing,
// so it's important to give it a special iterator.
type Null struct {
	uid  uint64
	tags graph.Tagger
}

// Fairly useless New function.
func NewNull() *Null {
	return &Null{uid: NextUID()}
}

func (it *Null) UID() uint64 {
	return it.uid
}

func (it *Null) Tagger() *graph.Tagger {
	return &it.tags
}

// Fill the map based on the tags assigned to this iterator.
func (it *Null) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *Null) Check(graph.Value) bool {
	return false
}

func (it *Null) Clone() graph.Iterator { return NewNull() }

// Name the null iterator.
func (it *Null) Type() graph.Type { return graph.Null }

// A good iterator will close itself when it returns true.
// Null has nothing it needs to do.
func (it *Null) Optimize() (graph.Iterator, bool) { return it, false }

// Print the null iterator.
func (it *Null) DebugString(indent int) string {
	return strings.Repeat(" ", indent) + "(null)"
}

func (it *Null) CanNext() bool { return true }

func (it *Null) Next() (graph.Value, bool) {
	return nil, false
}

func (it *Null) Result() graph.Value {
	return nil
}

func (it *Null) ResultTree() *graph.ResultTree {
	return graph.NewResultTree(it.Result())
}

func (it *Null) SubIterators() []graph.Iterator {
	return nil
}

func (it *Null) NextResult() bool {
	return false
}

func (it *Null) Size() (int64, bool) {
	return 0, true
}

func (it *Null) Reset() {}

func (it *Null) Close() {}

// A null iterator costs nothing. Use it!
func (it *Null) Stats() graph.IteratorStats {
	return graph.IteratorStats{}
}
