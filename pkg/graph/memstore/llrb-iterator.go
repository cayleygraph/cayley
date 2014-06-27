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

package memstore

import (
	"fmt"
	"github.com/google/cayley/pkg/graph"
	"github.com/petar/GoLLRB/llrb"
	"math"
	"strings"
)

type LlrbIterator struct {
	graph.BaseIterator
	tree      *llrb.LLRB
	data      string
	isRunning bool
	iterLast  Int64
}

type Int64 int64

func (i Int64) Less(than llrb.Item) bool {
	return i < than.(Int64)
}

func IterateOne(tree *llrb.LLRB, last Int64) Int64 {
	var next Int64
	tree.AscendGreaterOrEqual(last, func(i llrb.Item) bool {
		if i.(Int64) == last {
			return true
		} else {
			next = i.(Int64)
			return false
		}
	})
	return next
}

func NewLlrbIterator(tree *llrb.LLRB, data string) *LlrbIterator {
	var it LlrbIterator
	graph.BaseIteratorInit(&it.BaseIterator)
	it.tree = tree
	it.iterLast = Int64(-1)
	it.data = data
	return &it
}

func (it *LlrbIterator) Reset() {
	it.iterLast = Int64(-1)
}

func (it *LlrbIterator) Clone() graph.Iterator {
	var new_it = NewLlrbIterator(it.tree, it.data)
	new_it.CopyTagsFrom(it)
	return new_it
}

func (it *LlrbIterator) Close() {}

func (it *LlrbIterator) Next() (graph.TSVal, bool) {
	graph.NextLogIn(it)
	if it.tree.Max() == nil || it.Last == int64(it.tree.Max().(Int64)) {
		return graph.NextLogOut(it, nil, false)
	}
	it.iterLast = IterateOne(it.tree, it.iterLast)
	it.Last = int64(it.iterLast)
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
