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

// Defines the Variable iterator. A Variable iterator shares state with other variable
// iterators of the same name (it.varName). Consistency is maintained by making one iterator
// per variable (name) the 'binder', and making all other 'users'.

// The binder updates the value of the variable when its Next() method is called. It updates
// value of the variable in the iteration context, making it available to users.
// Next() is not available to users because there is no guarantee of consistency when multiple
// iterators can update the variable's value.

// Iterator trees with variable iterators have to be reorder to make sure users never have
// Next() called on them. This being the case, Contains() is undefined on binders to ensure
// that the different types of iterators keep their proper place in the tree. However, Contains()
// on binders should be easy to implement should the need arise.

// The result of Contains() on a user depends on the state of the iteration context, so variables
// cannot be used with iterators that cache contains results like the Materialize iterator.
import (
	"sort"

	"github.com/codelingo/cayley/graph"
)

// A Variable iterator consists of a name and an indication of whether it is a binder or a user.
// The other state necessary for iteration is handled by the iteration context.
type Variable struct {
	uid       uint64
	tags      graph.Tagger
	varName   string
	lastIndex int
	result    graph.Value
	isBinder  bool
	qs        graph.QuadStore
}

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
	out := NewVariable(it.qs, it.varName)
	out.tags.CopyFrom(it)
	return out
}

func (it *Variable) Describe() graph.Description {
	fixed := make([]string, 0, len(it.tags.Fixed()))
	for k := range it.tags.Fixed() {
		fixed = append(fixed, k)
	}
	s, _ := it.Size()
	sort.Strings(fixed)
	return graph.Description{
		UID:  it.UID(),
		Name: it.varName,
		Type: it.Type(),
		Tags: fixed,
		Size: s,
	}
}

// Register this iterator as a Variable iterator.
func (it *Variable) Type() graph.Type { return graph.Variable }

// Contains checks if the passed value is equal to the current value of the variable.
func (it *Variable) Contains(ctx *graph.IterationContext, v graph.Value) bool {
	graph.ContainsLogIn(it, v)
	if ctx.BindVariable(it.qs, it.varName) || it.isBinder {
		panic("Reorder iterator tree for variables. Contains should not bind a variable.")
	}

	if v == ctx.CurrentValue(it.varName) {
		return graph.ContainsLogOut(it, v, true)
	}

	return graph.ContainsLogOut(it, v, false)

}

// Next advances the value of the variable on the iteration context.
func (it *Variable) Next(ctx *graph.IterationContext) bool {
	graph.NextLogIn(it)

	if ctx.BindVariable(it.qs, it.varName) {
		it.isBinder = true
		ctx.Next(it.varName)
	}

	if it.isBinder {
		if ctx.Next(it.varName) {
			it.result = ctx.CurrentValue(it.varName)
			return graph.NextLogOut(it, true)
		}
		it.result = nil
		return graph.NextLogOut(it, false)
	}

	panic("query should be reordered so that only binders call next")
}

func (it *Variable) Err() error {
	return nil
}

func (it *Variable) Result() graph.Value {
	return it.result
}

func (it *Variable) NextPath(ctx *graph.IterationContext) bool {
	return false
}

// No sub-iterators.
func (it *Variable) SubIterators() []graph.Iterator {
	return []graph.Iterator{}
}

// There is no (apparent) optimization for a variable iterator, because most of its information is stored
// in the iteration context.
func (it *Variable) Optimize() (graph.Iterator, bool) {
	return it, false
}

// Size is unclear because it is largely store on the iteratorContext.
func (it *Variable) Size() (int64, bool) {
	return int64(0), true
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
