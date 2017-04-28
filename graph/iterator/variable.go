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
	"fmt"
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
	// panic("You need to reorderIteratorTree for variable iterators")
	// graph.DescribeIteratorTree(it, "")
	// TODO(BlakeMScurr) If we make the IterationContext values a slice for each possible
	graph.ContainsLogIn(it, v)
	// return graph.ContainsLogOut(it, v, true)
	if ctx.BindVariable(it.qs, it.varName) || it.isBinder {
		panic("Reorder iterator tree for variables. Contains should not bind a variable.")
	}

	currVar := ctx.CurrentValue(it.varName)
	nameOfCurr := it.qs.NameOf(currVar).String()
	nameOfArg := it.qs.NameOf(v).String()
	fmt.Println("current value is " + nameOfCurr)
	if nameOfCurr != "\"string\"" {
		fmt.Println("not string")
	} else {
		fmt.Println("is string")
	}
	if v == currVar {
		if nameOfCurr == "\"a\"" {
			fmt.Println("probably in loop")
		}
		fmt.Println("contains" + nameOfCurr)
		return graph.ContainsLogOut(it, v, true)
	}
	// fmt.Println("f - " + it.qs.NameOf(v).String())
	// fmt.Println("Actual value - " + it.qs.NameOf(ctx.CurrentValue(it.varName)).String())
	fmt.Println("does not contain" + nameOfArg)
	if nameOfArg == "\"2_32\"^^<key>" {
		fmt.Println("last")
	}

	return graph.ContainsLogOut(it, v, false)

	// if it.isBinder {
	// 	if it.subIt.Contains(ctx, v) {
	// 		fmt.Println("set value to" + it.qs.NameOf(v).String())
	// 		if it.qs.NameOf(v).String() == "\"b\"" {
	// 			fmt.Println("It's b!")
	// 		}
	// 		if it.qs.NameOf(v).String() == "\"c\"" {
	// 			fmt.Println("It's c!")
	// 		}
	// 		it.result = v
	// 		ctx.SetValue(it.varName, v)
	// 		return graph.ContainsLogOut(it, v, true)
	// 	}
	// 	return graph.ContainsLogOut(it, v, false)
	// }
	// currVar := ctx.CurrentValue(it.varName)
	// // currVar := it.qs.ValueOf(quad.String("a"))
	// fmt.Println(it.qs.NameOf(v).String())
	// fmt.Println("Argument" + it.qs.NameOf(v).String())
	// fmt.Println("current value" + it.qs.NameOf(currVar).String())
	// // return graph.ContainsLogOut(it, v, true)
	// // it.qs.ValueOf(quad.String("a"))
	// if v == currVar {
	// 	fmt.Println(it.qs.NameOf(v).String())
	// 	return graph.ContainsLogOut(it, v, true)
	// }
	// fmt.Println("f - " + it.qs.NameOf(v).String())
	// fmt.Println("Actual value - " + it.qs.NameOf(ctx.CurrentValue(it.varName)).String())
	// return graph.ContainsLogOut(it, v, false)
}

// Next advances the iterator.
func (it *Variable) Next(ctx *graph.IterationContext) bool {
	graph.NextLogIn(it)

	if ctx.BindVariable(it.qs, it.varName) {
		it.isBinder = true
		ctx.Next(it.varName)
	}

	if it.isBinder {
		if ctx.Next(it.varName) {
			it.result = ctx.CurrentValue(it.varName)
			fmt.Println("1")
			name := it.qs.NameOf(it.result).String()
			fmt.Println(name)
			if name == "\"b\"" {
				fmt.Println("We have b")
			}
			// if name != "\"b\"" && name != "\"a\"" {
			// 	return it.Next(ctx)
			// }
			return graph.NextLogOut(it, true)
		}
		// fmt.Println("2")
		it.result = nil
		return graph.NextLogOut(it, false)
	}
	panic("query should be reordered so that only binders call next")
	newRes := ctx.CurrentValue(it.varName)
	b := it.result != newRes
	it.result = newRes

	return graph.NextLogOut(it, b)
}

// // Next advances the iterator.
// func (it *Variable) Next(ctx *graph.IterationContext) bool {
// 	graph.NextLogIn(it)

// 	var newValue graph.Value
// 	if !ctx.BindVariable(it.qs, it.varName) {
// 		newValue = ctx.CurrentValue(it.varName)
// 	} else {
// 		newValue = nil
// 	}

// 	if newValue == it.result {
// 		if ctx.Next(it.varName) {
// 			it.result = ctx.CurrentValue(it.varName)
// 			fmt.Println("1")
// 			return graph.NextLogOut(it, true)
// 		}
// 		fmt.Println("2")
// 		return graph.NextLogOut(it, false)
// 	}

// 	it.result = newValue
// 	b := it.result == nil

// 	s := strconv.FormatBool(b)
// 	fmt.Println("3" + s)
// 	if !b {
// 		fmt.Println("not b")
// 	}
// 	return graph.NextLogOut(it, !b)
// }

// func (it *Variable) Next(ctx *graph.IterationContext) bool {
// 	graph.NextLogIn(it)
// 	if ctx.BindVariable(it.varName) {
// 		it.subIt = it.qs.NodesAllIterator()
// 		it.isBinder = true
// 	}

// 	if it.isBinder {
// 		if it.subIt.Next(ctx) {
// 			it.result = it.subIt.Result()
// 			fmt.Println("bind" + it.qs.NameOf(it.result).String())
// 			ctx.SetValue(it.varName, it.result)
// 			return graph.NextLogOut(it, true)
// 		}
// 		ctx.SetValue(it.varName, nil)
// 		return graph.NextLogOut(it, false)
// 	}

// 	newValue := ctx.CurrentValue(it.varName)
// 	if newValue == it.result {
// 		// it.result = nil
// 		return graph.NextLogOut(it, true)
// 	}
// 	it.result = newValue
// 	fmt.Println("user" + it.qs.NameOf(newValue).String())
// 	if it.qs.NameOf(newValue).String() != "\".\"" && it.qs.NameOf(newValue).String() != "\".lingo\"" {
// 		fmt.Println("C!")
// 	}
// 	if newValue == nil {
// 		return graph.NextLogOut(it, false)
// 	}
// 	return graph.NextLogOut(it, true)
// }

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
