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

// A simple iterator that, when first called Contains() or Next() upon, materializes the whole subiterator, stores it locally, and responds. Essentially a cache.

import (
	"fmt"
	"strings"

	"github.com/barakmich/glog"

	"github.com/google/cayley/graph"
)

var abortMaterializeAt = 1000

type result struct {
	id   graph.Value
	tags map[string]graph.Value
}

type hasher interface {
	Hasher() interface{}
}

type Materialize struct {
	uid         uint64
	tags        graph.Tagger
	containsMap map[graph.Value]int
	values      [][]result
	index       int
	subindex    int
	subIt       graph.Iterator
	hasRun      bool
	aborted     bool
}

func NewMaterialize(sub graph.Iterator) *Materialize {
	return &Materialize{
		uid:         NextUID(),
		containsMap: make(map[graph.Value]int),
		subIt:       sub,
		index:       -1,
	}
}

func (it *Materialize) UID() uint64 {
	return it.uid
}

func (it *Materialize) Reset() {
	it.subIt.Reset()
	it.index = -1
}

func (it *Materialize) Close() {
	it.subIt.Close()
	it.containsMap = nil
	it.values = nil
	it.hasRun = false
}

func (it *Materialize) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *Materialize) TagResults(dst map[string]graph.Value) {
	if !it.hasRun {
		return
	}
	if it.aborted {
		it.subIt.TagResults(dst)
		return
	}
	if it.Result() == nil {
		return
	}
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}
	for tag, value := range it.values[it.index][it.subindex].tags {
		dst[tag] = value
	}
}

func (it *Materialize) Clone() graph.Iterator {
	out := NewMaterialize(it.subIt.Clone())
	out.tags.CopyFrom(it)
	if it.hasRun {
		out.hasRun = true
		out.aborted = it.aborted
		out.values = it.values
		out.containsMap = it.containsMap
	}
	return out
}

// Print some information about the iterator.
func (it *Materialize) DebugString(indent int) string {
	return fmt.Sprintf("%s(%s tags: %s Size: %d\n%s)",
		strings.Repeat(" ", indent),
		it.Type(),
		it.tags.Tags(),
		len(it.values),
		it.subIt.DebugString(indent+4),
	)
}

// Register this iterator as a Materialize iterator.
func (it *Materialize) Type() graph.Type { return graph.Materialize }

// DEPRECATED
func (it *Materialize) ResultTree() *graph.ResultTree {
	tree := graph.NewResultTree(it.Result())
	tree.AddSubtree(it.subIt.ResultTree())
	return tree
}

func (it *Materialize) Result() graph.Value {
	if len(it.values) == 0 {
		return nil
	}
	if it.index == -1 {
		return nil
	}
	if it.index >= len(it.values) {
		return nil
	}
	return it.values[it.index][it.subindex].id
}

func (it *Materialize) SubIterators() []graph.Iterator {
	return []graph.Iterator{it.subIt}
}

func (it *Materialize) Optimize() (graph.Iterator, bool) {
	newSub, changed := it.subIt.Optimize()
	if changed {
		it.subIt = newSub
		if it.subIt.Type() == graph.Null {
			return it.subIt, true
		}
	}
	return it, false
}

// Size is the number of values stored, if we've got them all.
// Otherwise, guess based on the size of the subiterator.
func (it *Materialize) Size() (int64, bool) {
	if it.hasRun {
		return int64(len(it.values)), true
	}
	return it.subIt.Size()
}

// The entire point of Materialize is to amortize the cost by
// putting it all up front.
func (it *Materialize) Stats() graph.IteratorStats {
	overhead := int64(2)
	size, _ := it.Size()
	subitStats := it.subIt.Stats()
	return graph.IteratorStats{
		ContainsCost: overhead * subitStats.NextCost,
		NextCost:     overhead * subitStats.NextCost,
		Size:         size,
	}
}

func (it *Materialize) Next() (graph.Value, bool) {
	graph.NextLogIn(it)
	if !it.hasRun {
		it.materializeSet()
	}
	if it.aborted {
		return graph.Next(it.subIt)
	}

	it.index++
	it.subindex = 0
	if it.index >= len(it.values) {
		return graph.NextLogOut(it, nil, false)
	}
	return graph.NextLogOut(it, it.Result(), true)
}

func (it *Materialize) Contains(v graph.Value) bool {
	graph.ContainsLogIn(it, v)
	if !it.hasRun {
		it.materializeSet()
	}
	if it.aborted {
		return it.subIt.Contains(v)
	}
	key := v
	if h, ok := v.(hasher); ok {
		key = h.Hasher()
	}
	if i, ok := it.containsMap[key]; ok {
		it.index = i
		it.subindex = 0
		return graph.ContainsLogOut(it, v, true)
	}
	return graph.ContainsLogOut(it, v, false)
}

func (it *Materialize) NextResult() bool {
	if !it.hasRun {
		it.materializeSet()
	}
	if it.aborted {
		return it.subIt.NextResult()
	}

	it.subindex++
	if it.subindex >= len(it.values[it.index]) {
		// Don't go off the end of the world
		it.subindex--
		return false
	}
	return true
}

func (it *Materialize) materializeSet() {
	i := 0
	for {
		id, ok := graph.Next(it.subIt)
		if !ok {
			break
		}
		i += 1
		if i > abortMaterializeAt {
			it.aborted = true
			break
		}
		val := id
		if h, ok := id.(hasher); ok {
			val = h.Hasher()
		}
		if _, ok := it.containsMap[val]; !ok {
			it.containsMap[val] = len(it.values)
			it.values = append(it.values, nil)
		}
		index := it.containsMap[val]
		tags := make(map[string]graph.Value)
		it.subIt.TagResults(tags)
		it.values[index] = append(it.values[index], result{id: id, tags: tags})
		for it.subIt.NextResult() == true {
			tags := make(map[string]graph.Value)
			it.subIt.TagResults(tags)
			it.values[index] = append(it.values[index], result{id: id, tags: tags})
		}
	}
	if it.aborted {
		it.values = nil
		it.containsMap = nil
		it.subIt.Reset()
	}
	glog.Infof("Materialization List %d: %#v", it.values)
	it.hasRun = true
}
