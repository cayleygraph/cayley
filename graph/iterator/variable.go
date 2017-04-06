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

// Defines a fairly uncommon iterator, the variable iterator. The variable iterator is like the fixed
// iterator, except that rather than having a set of fixed values to compare, it has a single variable
// value stored inside its variableUser or variableBinder that is updated dynamically as the path is
// executed.
//
// A variable iterator requires an Equality function to be passed to it, by reason that graph.Value, the
// opaque Quad store value, may not answer to ==.

import (
	"fmt"
	"sort"

	"github.com/codelingo/cayley/graph"
	"github.com/codelingo/cayley/quad"
)

// A Variable iterator consists of a reference to a variable containing a changing value,
// an index (where it is in the process of Next()ing) and an equality function as defined
// by the fixed iterator type.
type Variable struct {
	uid            uint64
	tags           graph.Tagger
	variableUser   *graph.VarUser
	variableBinder *graph.VarBinder
	subit          graph.Iterator
	lastIndex      int
	cmp            Equality
	result         graph.Value
	dir            quad.Direction
}

// NewVariable creates a new Variable iterator with a custom comparator. You can create a variable iterator
// that binds its variable, just uses its variable, or doesn't have an underlying variable.
func NewVariable(cmp Equality) *Variable {
	it := &Variable{
		uid: NextUID(),
		cmp: cmp,
	}
	return it
}

// Use makes the Variable iterator refer to the the value refered by the VarUser passed in.
func (it *Variable) Use(vu *graph.VarUser) {
	it.variableUser = vu
	it.variableBinder = nil
}

func (it *Variable) Bind(vb *graph.VarBinder, subit graph.Iterator) {
	it.variableBinder = vb
	it.variableUser = nil
	it.subit = subit
}

func (it *Variable) getValue() graph.Value {
	if it.variableBinder != nil {
		return it.variableBinder.GetCurrentValue()
	} else if it.variableUser != nil {
		return it.variableUser.GetCurrentValue()
	}
	return nil
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

// It's not clear that the normal tagging logic works for variables
func (it *Variable) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

// Clone creates a new iterator refering to the same variable. If the current iterator is a
// variable binder, Clone returns a varible user refering to the same variable.
func (it *Variable) Clone() graph.Iterator {
	out := NewVariable(it.cmp)
	if it.variableBinder != nil {
		out.variableBinder = it.variableBinder
	} else if it.variableUser != nil {
		out.variableUser = it.variableUser
	}
	out.tags.CopyFrom(it)
	return out
}

// It might be useful to have size refer to the number of graph values that this iterator
// been bound to
func (it *Variable) Describe() graph.Description {
	var value string
	size, _ := it.Size()
	fixed := make([]string, 0, len(it.tags.Fixed()))
	for k := range it.tags.Fixed() {
		fixed = append(fixed, k)
	}
	sort.Strings(fixed)
	return graph.Description{
		UID:  it.UID(),
		Name: value,
		Type: it.Type(),
		Tags: fixed,
		Size: size,
	}
}

// Register this iterator as a Variable iterator.
func (it *Variable) Type() graph.Type { return graph.Variable }

// Check if the passed value is equal to the value stored in the variable.
func (it *Variable) Contains(v graph.Value) bool {
	graph.ContainsLogIn(it, v)
	if it.cmp(it.getValue(), v) {
		it.result = v
		return graph.ContainsLogOut(it, v, true)
	}
	return graph.ContainsLogOut(it, v, false)
}

// Next advances the iterator. This function essentially forks on a boolean, so we should
// refactor into two next functions (thus, potentially, two iterators).
var anint int

func (it *Variable) Next() bool {
	graph.NextLogIn(it)
	if it.variableUser != nil {
		it.result = it.variableUser.GetCurrentValue()
		return graph.NextLogOut(it, true)
	} else if it.variableBinder != nil {
		if it.subit.Next() {
			val := it.subit.Result()
			it.variableBinder.SetNewValue(val)
			it.result = val
			anint++
			fmt.Println(anint)
			return graph.NextLogOut(it, true)
		}
		return graph.NextLogOut(it, false)
	}
	return graph.NextLogOut(it, false)
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
	if it.variableBinder == nil && it.variableUser == nil {
		return &Null{}, true
	}

	return it, false
}

// Size is the number of values stored.
func (it *Variable) Size() (int64, bool) {
	var size int64
	if it.getValue() != nil {
		size = 1
	}
	return size, true
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
