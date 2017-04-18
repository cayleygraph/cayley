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

// Defines the Variable iterator. A variable iterator is one whose value may depend on the
// current value (Result()) of other iterators. If no other variable iterator has defined
// the value for the variable, you take a
//
// A fixed iterator requires an Equality function to be passed to it, by reason that graph.Value, the
// opaque Quad store value, may not answer to ==.

import (
	"sort"

	"github.com/codelingo/cayley/graph"
)

// A Variable iterator consists of it's values, an index (where it is in the process of Next()ing) and
// an equality function.
type Variable struct {
	uid       uint64
	tags      graph.Tagger
	values    []graph.Value
	varName   string
	lastIndex int
	subIt     graph.Iterator
	result    graph.Value
	isBinder  bool
	qs        graph.QuadStore
}

// NewVariable creates a new Variable iterator with a custom comparator.
func NewVariable(qs graph.QuadStore, name string) *Variable {
	it := &Variable{
		uid:     NextUID(),
		varName: name,
		qs:      qs,
	}
	return it
}

func (it *Variable) UID() uint64 {
	return it.uid
}

func (it *Variable) Reset() {
	it.lastIndex = 0
}

func (it *Variable) Close() error {
	return nil
}

func (it *Variable) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *Variable) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *Variable) Clone() graph.Iterator {
	// The cloned variable will just be a user, not a binder, so we don't need to passed
	// the it.values
	out := NewVariable(it.qs, it.varName)
	out.tags.CopyFrom(it)
	return out
}

func (it *Variable) Describe() graph.Description {
	fixed := make([]string, 0, len(it.tags.Fixed()))
	for k := range it.tags.Fixed() {
		fixed = append(fixed, k)
	}
	sort.Strings(fixed)
	return graph.Description{
		UID:  it.UID(),
		Name: it.varName,
		Type: it.Type(),
		Tags: fixed,
		// Zero if this is not the binder
		Size: int64(len(it.values)),
	}
}

// Register this iterator as a Variable iterator.
func (it *Variable) Type() graph.Type { return graph.Variable }

// Contains checks if the passed value is equal to one of the values stored in the iterator.
func (it *Variable) Contains(ctx *graph.IterationContext, v graph.Value) bool {
	// TODO(BlakeMScurr) If we make the IterationContext values a slice for each possible
	graph.ContainsLogIn(it, v)
	if ctx.BindVariable(it.varName) {
		it.subIt = it.qs.NodesAllIterator()
		it.isBinder = true
	}

	if it.isBinder {
		if it.subIt.Contains(ctx, v) {
			ctx.SetValue(it.varName, v)
			return graph.ContainsLogOut(it, v, true)
		}
		return graph.ContainsLogOut(it, v, false)
	}

	if v == ctx.CurrentValue(it.varName) {
		return graph.ContainsLogOut(it, v, true)
	}
	return graph.ContainsLogOut(it, v, false)
}

// Next advances the iterator.
func (it *Variable) Next(ctx *graph.IterationContext) bool {
	graph.NextLogIn(it)
	if ctx.BindVariable(it.varName) {
		it.subIt = it.qs.NodesAllIterator()
		it.isBinder = true
	}

	if it.isBinder {
		if it.subIt.Next(ctx) {
			it.result = it.subIt.Result()
			ctx.SetValue(it.varName, it.result)
			return graph.NextLogOut(it, true)
		}
		ctx.SetValue(it.varName, nil)
		return graph.NextLogOut(it, false)
	}

	newValue := ctx.CurrentValue(it.varName)
	if newValue == it.result {
		it.result = nil
		return graph.NextLogOut(it, false)
	}
	it.result = newValue
	return graph.NextLogOut(it, true)
}

func (it *Variable) Err() error {
	return nil
}

func (it *Variable) Result() graph.Value {
	return it.result
}

func (it *Variable) NextPath() bool {
	return false
}

// No sub-iterators.
func (it *Variable) SubIterators() []graph.Iterator {
	return nil
}

// Optimize() for a Variable iterator is simple. Returns a Null iterator if it's empty
// (so that other iterators upstream can treat this as null) or there is no
// optimization.
func (it *Variable) Optimize() (graph.Iterator, bool) {
	if len(it.values) == 1 && it.values[0] == nil {
		return &Null{}, true
	}

	return it, false
}

// Size is the number of values stored.
func (it *Variable) Size() (int64, bool) {
	return int64(len(it.values)), true
}

// As we right now have to scan the entire list, Next and Contains are linear with the
// size. However, a better data structure could remove these limits.
func (it *Variable) Stats() graph.IteratorStats {
	s, exact := it.Size()
	return graph.IteratorStats{
		ContainsCost: s,
		NextCost:     s,
		Size:         s,
		ExactSize:    exact,
	}
}

var _ graph.Iterator = &Variable{}
