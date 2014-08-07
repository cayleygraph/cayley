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
	"math"
	"strings"

	"github.com/petar/GoLLRB/llrb"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
)

type Iterator struct {
	uid       uint64
	tags      graph.Tagger
	tree      *llrb.LLRB
	data      string
	isRunning bool
	iterLast  Int64
	result    graph.Value
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

func NewLlrbIterator(tree *llrb.LLRB, data string) *Iterator {
	return &Iterator{
		uid:      iterator.NextUID(),
		tree:     tree,
		iterLast: Int64(-1),
		data:     data,
	}
}

func (it *Iterator) UID() uint64 {
	return it.uid
}

func (it *Iterator) Reset() {
	it.iterLast = Int64(-1)
}

func (it *Iterator) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *Iterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *Iterator) Clone() graph.Iterator {
	m := NewLlrbIterator(it.tree, it.data)
	m.tags.CopyFrom(it)
	return m
}

func (it *Iterator) Close() {}

func (it *Iterator) Next() bool {
	graph.NextLogIn(it)
	if it.tree.Max() == nil || it.result == int64(it.tree.Max().(Int64)) {
		return graph.NextLogOut(it, nil, false)
	}
	it.iterLast = IterateOne(it.tree, it.iterLast)
	it.result = int64(it.iterLast)
	return graph.NextLogOut(it, it.result, true)
}

func (it *Iterator) ResultTree() *graph.ResultTree {
	return graph.NewResultTree(it.Result())
}

func (it *Iterator) Result() graph.Value {
	return it.result
}

func (it *Iterator) NextPath() bool {
	return false
}

// No subiterators.
func (it *Iterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *Iterator) Size() (int64, bool) {
	return int64(it.tree.Len()), true
}

func (it *Iterator) Contains(v graph.Value) bool {
	graph.ContainsLogIn(it, v)
	if it.tree.Has(Int64(v.(int64))) {
		it.result = v
		return graph.ContainsLogOut(it, v, true)
	}
	return graph.ContainsLogOut(it, v, false)
}

func (it *Iterator) DebugString(indent int) string {
	size, _ := it.Size()
	return fmt.Sprintf("%s(%s tags:%s size:%d %s)", strings.Repeat(" ", indent), it.Type(), it.tags.Tags(), size, it.data)
}

var memType graph.Type

func init() {
	memType = graph.RegisterIterator("llrb")
}

func Type() graph.Type { return memType }

func (it *Iterator) Type() graph.Type { return memType }

func (it *Iterator) Sorted() bool { return true }

func (it *Iterator) Optimize() (graph.Iterator, bool) {
	return it, false
}

func (it *Iterator) Stats() graph.IteratorStats {
	return graph.IteratorStats{
		ContainsCost: int64(math.Log(float64(it.tree.Len()))) + 1,
		NextCost:     1,
		Size:         int64(it.tree.Len()),
	}
}
