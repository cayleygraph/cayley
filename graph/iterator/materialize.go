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
	"context"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
)

var _ graph.Iterator = &Materialize{}

const MaterializeLimit = 1000

type result struct {
	id   graph.Ref
	tags map[string]graph.Ref
}

type Materialize struct {
	containsMap map[interface{}]int
	values      [][]result
	actualSize  int64
	expectSize  int64
	index       int
	subindex    int
	subIt       graph.Iterator
	hasRun      bool
	aborted     bool
	runstats    graph.IteratorStats
	err         error
}

func NewMaterialize(sub graph.Iterator) *Materialize {
	return NewMaterializeWithSize(sub, 0)
}

func NewMaterializeWithSize(sub graph.Iterator, size int64) *Materialize {
	return &Materialize{
		expectSize:  size,
		containsMap: make(map[interface{}]int),
		subIt:       sub,
		index:       -1,
	}
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

func (it *Materialize) TagResults(dst map[string]graph.Ref) {
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
	for tag, value := range it.values[it.index][it.subindex].tags {
		dst[tag] = value
	}
}

func (it *Materialize) String() string {
	return "Materialize"
}

func (it *Materialize) Result() graph.Ref {
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
		if _, ok := it.subIt.(*Null); ok {
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
	var (
		size  int64
		exact bool
	)
	if it.expectSize > 0 {
		size, exact = it.expectSize, false
	} else {
		size, exact = it.Size()
	}
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

func (it *Materialize) Next(ctx context.Context) bool {
	it.runstats.Next += 1
	if !it.hasRun {
		it.materializeSet(ctx)
	}
	if it.err != nil {
		return false
	}
	if it.aborted {
		n := it.subIt.Next(ctx)
		it.err = it.subIt.Err()
		return n
	}

	it.index++
	it.subindex = 0
	if it.index >= len(it.values) {
		return false
	}
	return true
}

func (it *Materialize) Err() error {
	return it.err
}

func (it *Materialize) Contains(ctx context.Context, v graph.Ref) bool {
	it.runstats.Contains += 1
	if !it.hasRun {
		it.materializeSet(ctx)
	}
	if it.err != nil {
		return false
	}
	if it.aborted {
		return it.subIt.Contains(ctx, v)
	}
	key := graph.ToKey(v)
	if i, ok := it.containsMap[key]; ok {
		it.index = i
		it.subindex = 0
		return true
	}
	return false
}

func (it *Materialize) NextPath(ctx context.Context) bool {
	if !it.hasRun {
		it.materializeSet(ctx)
	}
	if it.err != nil {
		return false
	}
	if it.aborted {
		return it.subIt.NextPath(ctx)
	}

	it.subindex++
	if it.subindex >= len(it.values[it.index]) {
		// Don't go off the end of the world
		it.subindex--
		return false
	}
	return true
}

func (it *Materialize) materializeSet(ctx context.Context) {
	i := 0
	mn := 0
	for it.subIt.Next(ctx) {
		i++
		if i > MaterializeLimit {
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
		tags := make(map[string]graph.Ref, mn)
		it.subIt.TagResults(tags)
		if n := len(tags); n > mn {
			n = mn
		}
		it.values[index] = append(it.values[index], result{id: id, tags: tags})
		it.actualSize += 1
		for it.subIt.NextPath(ctx) {
			i++
			if i > MaterializeLimit {
				it.aborted = true
				break
			}
			tags := make(map[string]graph.Ref, mn)
			it.subIt.TagResults(tags)
			if n := len(tags); n > mn {
				n = mn
			}
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
