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

package graph_memstore

import (
	"fmt"
	"github.com/petar/GoLLRB/llrb"
	"graph"
	"math"
	"strings"
)

type LlrbIterator struct {
	graph.BaseIterator
	tree      *llrb.LLRB
	values    chan llrb.Item
	another   chan bool
	data      string
	isRunning bool
}

type Int64 int64

func (i Int64) Less(than llrb.Item) bool {
	return i < than.(Int64)
}

func IterateAll(tree *llrb.LLRB, c chan llrb.Item, another chan bool) {
	tree.AscendGreaterOrEqual(Int64(-1), func(i llrb.Item) bool {
		want_more := <-another
		if want_more {
			c <- i
			return true
		}
		return false
	})
}

func NewLlrbIterator(tree *llrb.LLRB, data string) *LlrbIterator {
	var it LlrbIterator
	graph.BaseIteratorInit(&it.BaseIterator)
	it.tree = tree
	it.isRunning = false
	it.values = make(chan llrb.Item)
	it.another = make(chan bool, 1)
	it.data = data
	return &it
}

func (it *LlrbIterator) Reset() {
	if it.another != nil {
		it.another <- false
		close(it.another)
	}
	it.another = nil
	if it.values != nil {
		close(it.values)
	}
	it.values = nil
	it.isRunning = false
	it.another = make(chan bool)
	it.values = make(chan llrb.Item)
}

func (it *LlrbIterator) Clone() graph.Iterator {
	var new_it = NewLlrbIterator(it.tree, it.data)
	new_it.CopyTagsFrom(it)
	return new_it
}

func (it *LlrbIterator) Close() {
	if it.another != nil {
		it.another <- false
		close(it.another)
	}
	it.another = nil
	if it.values != nil {
		close(it.values)
	}
	it.values = nil
}

func (it *LlrbIterator) Next() (graph.TSVal, bool) {
	graph.NextLogIn(it)
	// Little hack here..
	if !it.isRunning {
		go IterateAll(it.tree, it.values, it.another)
		it.isRunning = true
	}
	last := int64(0)
	if it.Last != nil {
		last = it.Last.(int64)
	}
	if it.tree.Max() == nil || last == int64(it.tree.Max().(Int64)) {
		return graph.NextLogOut(it, nil, false)
	}
	it.another <- true
	val := <-it.values
	it.Last = int64(val.(Int64))
	return graph.NextLogOut(it, it.Last, true)
}

func (it *LlrbIterator) Size() (int64, bool) {
	return int64(it.tree.Len()), true
}

func (it *LlrbIterator) Check(v graph.TSVal) bool {
	graph.CheckLogIn(it, v)
	if it.tree.Has(Int64(v.(int64))) {
		it.Last = v
		return graph.CheckLogOut(it, v, true)
	}
	return graph.CheckLogOut(it, v, false)
}

func (it *LlrbIterator) DebugString(indent int) string {
	size, _ := it.Size()
	return fmt.Sprintf("%s(%s tags:%s size:%d %s)", strings.Repeat(" ", indent), it.Type(), it.Tags(), size, it.data)
}

func (it *LlrbIterator) Type() string {
	return "llrb"
}
func (it *LlrbIterator) Sorted() bool {
	return true
}
func (it *LlrbIterator) Optimize() (graph.Iterator, bool) {
	return it, false
}

func (it *LlrbIterator) GetStats() *graph.IteratorStats {
	return &graph.IteratorStats{
		CheckCost: int64(math.Log(float64(it.tree.Len()))) + 1,
		NextCost:  1,
		Size:      int64(it.tree.Len()),
	}
}
