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
	"github.com/cayleygraph/cayley/clog"

	"github.com/cayleygraph/cayley/graph"
)

var abortMaterializeAt = 1000

type result struct {
	id   graph.Value
	tags map[string]graph.Value
}

type Materialize struct {
	uid         uint64
	tags        graph.Tagger
	containsMap map[graph.Value]int
	values      [][]result
	actualSize  int64
	index       int
	subindex    int
	subIt       graph.Iterator
	hasRun      bool
	aborted     bool
	runstats    graph.IteratorStats
	err         error
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

func (it *Materialize) Close() error {
	it.containsMap = nil
	it.values = nil
	it.hasRun = false
	return it.subIt.Close()
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
		for _, tag := range it.tags.Tags() {
			dst[tag] = it.Result()
		}
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
		out.err = it.err
		out.values = it.values
		out.containsMap = it.containsMap
		out.actualSize = it.actualSize
	}
	return out
}

func (it *Materialize) Describe() graph.Description {
	primary := it.subIt.Describe()
	return graph.Description{
		UID:      it.UID(),
		Type:     it.Type(),
		Tags:     it.tags.Tags(),
		Size:     int64(len(it.values)),
		Iterator: &primary,
	}
}

// Register this iterator as a Materialize iterator.
func (it *Materialize) Type() graph.Type { return graph.Materialize }

func (it *Materialize) Result() graph.Value {
	if it.aborted {
		return it.subIt.Result()
	}
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
	if it.hasRun && !it.aborted {
		if clog.V(2) {
			clog.Infof("returning size %v", it.actualSize)
		}
		return it.actualSize, true
	}
	if clog.V(2) {
		clog.Infof("bailing size %v", it.actualSize)
	}
	return it.subIt.Size()
}

// The entire point of Materialize is to amortize the cost by
// putting it all up front.
func (it *Materialize) Stats() graph.IteratorStats {
	overhead := int64(2)
	size, exact := it.Size()
	subitStats := it.subIt.Stats()
	return graph.IteratorStats{
		ContainsCost: overhead * subitStats.NextCost,
		NextCost:     overhead * subitStats.NextCost,
		Size:         size,
		ExactSize:    exact,
		Next:         it.runstats.Next,
		Contains:     it.runstats.Contains,
	}
}

func (it *Materialize) Next() bool {
	graph.NextLogIn(it)
	it.runstats.Next += 1
	if !it.hasRun {
		it.materializeSet()
	}
	if it.err != nil {
		return false
	}
	if it.aborted {
		n := it.subIt.Next()
		it.err = it.subIt.Err()
		return n
	}

	it.index++
	it.subindex = 0
	if it.index >= len(it.values) {
		return graph.NextLogOut(it, false)
	}
	return graph.NextLogOut(it, true)
}

func (it *Materialize) Err() error {
	return it.err
}

func (it *Materialize) Contains(v graph.Value) bool {
	graph.ContainsLogIn(it, v)
	it.runstats.Contains += 1
	if !it.hasRun {
		it.materializeSet()
	}
	if it.err != nil {
		return false
	}
	if it.aborted {
		return it.subIt.Contains(v)
	}
	key := graph.ToKey(v)
	if i, ok := it.containsMap[key]; ok {
		it.index = i
		it.subindex = 0
		return graph.ContainsLogOut(it, v, true)
	}
	return graph.ContainsLogOut(it, v, false)
}

func (it *Materialize) NextPath() bool {
	if !it.hasRun {
		it.materializeSet()
	}
	if it.err != nil {
		return false
	}
	if it.aborted {
		return it.subIt.NextPath()
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
	for it.subIt.Next() {
		i++
		if i > abortMaterializeAt {
			it.aborted = true
			break
		}
		id := it.subIt.Result()
		val := graph.ToKey(id)
		if _, ok := it.containsMap[val]; !ok {
			it.containsMap[val] = len(it.values)
			it.values = append(it.values, nil)
		}
		index := it.containsMap[val]
		tags := make(map[string]graph.Value)
		it.subIt.TagResults(tags)
		it.values[index] = append(it.values[index], result{id: id, tags: tags})
		it.actualSize += 1
		for it.subIt.NextPath() {
			i++
			if i > abortMaterializeAt {
				it.aborted = true
				break
			}
			tags := make(map[string]graph.Value)
			it.subIt.TagResults(tags)
			it.values[index] = append(it.values[index], result{id: id, tags: tags})
			it.actualSize += 1
		}
	}
	it.err = it.subIt.Err()
	if it.err == nil && it.aborted {
		if clog.V(2) {
			clog.Infof("Aborting subiterator")
		}
		it.values = nil
		it.containsMap = nil
		it.subIt.Reset()
	}
	it.hasRun = true
}

var _ graph.Iterator = &Materialize{}
