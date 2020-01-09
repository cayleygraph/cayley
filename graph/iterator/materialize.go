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
	"github.com/cayleygraph/cayley/graph/refs"
)

const MaterializeLimit = 1000

type result struct {
	id   refs.Ref
	tags map[string]refs.Ref
}

type Materialize struct {
	sub        Shape
	expectSize int64
}

func NewMaterialize(sub Shape) *Materialize {
	return NewMaterializeWithSize(sub, 0)
}

func NewMaterializeWithSize(sub Shape, size int64) *Materialize {
	return &Materialize{
		sub:        sub,
		expectSize: size,
	}
}

func (it *Materialize) Iterate() Scanner {
	return newMaterializeNext(it.sub)
}

func (it *Materialize) Lookup() Index {
	return newMaterializeContains(it.sub)
}

func (it *Materialize) String() string {
	return "Materialize"
}

func (it *Materialize) SubIterators() []Shape {
	return []Shape{it.sub}
}

func (it *Materialize) Optimize(ctx context.Context) (Shape, bool) {
	newSub, changed := it.sub.Optimize(ctx)
	if changed {
		it.sub = newSub
		if IsNull(it.sub) {
			return it.sub, true
		}
	}
	return it, false
}

// The entire point of Materialize is to amortize the cost by
// putting it all up front.
func (it *Materialize) Stats(ctx context.Context) (Costs, error) {
	overhead := int64(2)
	var size refs.Size
	subitStats, err := it.sub.Stats(ctx)
	if it.expectSize > 0 {
		size = refs.Size{Value: it.expectSize, Exact: false}
	} else {
		size = subitStats.Size
	}
	return Costs{
		ContainsCost: overhead * subitStats.NextCost,
		NextCost:     overhead * subitStats.NextCost,
		Size:         size,
	}, err
}

type materializeNext struct {
	sub  Shape
	next Scanner

	containsMap map[interface{}]int
	values      [][]result
	index       int
	subindex    int
	hasRun      bool
	aborted     bool
	err         error
}

func newMaterializeNext(sub Shape) *materializeNext {
	return &materializeNext{
		containsMap: make(map[interface{}]int),
		sub:         sub,
		next:        sub.Iterate(),
		index:       -1,
	}
}

func (it *materializeNext) Close() error {
	it.containsMap = nil
	it.values = nil
	it.hasRun = false
	return it.next.Close()
}

func (it *materializeNext) TagResults(dst map[string]refs.Ref) {
	if !it.hasRun {
		return
	}
	if it.aborted {
		it.next.TagResults(dst)
		return
	}
	if it.Result() == nil {
		return
	}
	for tag, value := range it.values[it.index][it.subindex].tags {
		dst[tag] = value
	}
}

func (it *materializeNext) String() string {
	return "Materialize"
}

func (it *materializeNext) Result() refs.Ref {
	if it.aborted {
		return it.next.Result()
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

func (it *materializeNext) Next(ctx context.Context) bool {
	if !it.hasRun {
		it.materializeSet(ctx)
	}
	if it.err != nil {
		return false
	}
	if it.aborted {
		n := it.next.Next(ctx)
		it.err = it.next.Err()
		return n
	}

	it.index++
	it.subindex = 0
	if it.index >= len(it.values) {
		return false
	}
	return true
}

func (it *materializeNext) Err() error {
	return it.err
}

func (it *materializeNext) NextPath(ctx context.Context) bool {
	if !it.hasRun {
		it.materializeSet(ctx)
	}
	if it.err != nil {
		return false
	}
	if it.aborted {
		return it.next.NextPath(ctx)
	}

	it.subindex++
	if it.subindex >= len(it.values[it.index]) {
		// Don't go off the end of the world
		it.subindex--
		return false
	}
	return true
}

func (it *materializeNext) materializeSet(ctx context.Context) {
	i := 0
	mn := 0
	for it.next.Next(ctx) {
		i++
		if i > MaterializeLimit {
			it.aborted = true
			break
		}
		id := it.next.Result()
		val := refs.ToKey(id)
		if _, ok := it.containsMap[val]; !ok {
			it.containsMap[val] = len(it.values)
			it.values = append(it.values, nil)
		}
		index := it.containsMap[val]
		tags := make(map[string]refs.Ref, mn)
		it.next.TagResults(tags)
		if n := len(tags); n > mn {
			mn = n
		}
		it.values[index] = append(it.values[index], result{id: id, tags: tags})
		for it.next.NextPath(ctx) {
			i++
			if i > MaterializeLimit {
				it.aborted = true
				break
			}
			tags := make(map[string]refs.Ref, mn)
			it.next.TagResults(tags)
			if n := len(tags); n > mn {
				mn = n
			}
			it.values[index] = append(it.values[index], result{id: id, tags: tags})
		}
	}
	it.err = it.next.Err()
	if it.err == nil && it.aborted {
		if clog.V(2) {
			clog.Infof("Aborting subiterator")
		}
		it.values = nil
		it.containsMap = nil
		_ = it.next.Close()
		it.next = it.sub.Iterate()
	}
	it.hasRun = true
}

type materializeContains struct {
	next *materializeNext
	sub  Index // only set if aborted
}

func newMaterializeContains(sub Shape) *materializeContains {
	return &materializeContains{
		next: newMaterializeNext(sub),
	}
}

func (it *materializeContains) Close() error {
	err := it.next.Close()
	if it.sub != nil {
		if err2 := it.sub.Close(); err2 != nil && err == nil {
			err = err2
		}
	}
	return err
}

func (it *materializeContains) TagResults(dst map[string]refs.Ref) {
	if it.sub != nil {
		it.sub.TagResults(dst)
		return
	}
	it.next.TagResults(dst)
}

func (it *materializeContains) String() string {
	return "MaterializeContains"
}

func (it *materializeContains) Result() refs.Ref {
	if it.sub != nil {
		return it.sub.Result()
	}
	return it.next.Result()
}

func (it *materializeContains) Err() error {
	if err := it.next.Err(); err != nil {
		return err
	} else if it.sub == nil {
		return nil
	}
	return it.sub.Err()
}

func (it *materializeContains) run(ctx context.Context) {
	it.next.materializeSet(ctx)
	if it.next.aborted {
		it.sub = it.next.sub.Lookup()
	}
}

func (it *materializeContains) Contains(ctx context.Context, v refs.Ref) bool {
	if !it.next.hasRun {
		it.run(ctx)
	}
	if it.next.Err() != nil {
		return false
	}
	if it.sub != nil {
		return it.sub.Contains(ctx, v)
	}
	key := refs.ToKey(v)
	if i, ok := it.next.containsMap[key]; ok {
		it.next.index = i
		it.next.subindex = 0
		return true
	}
	return false
}

func (it *materializeContains) NextPath(ctx context.Context) bool {
	if !it.next.hasRun {
		it.run(ctx)
	}
	if it.next.Err() != nil {
		return false
	}
	if it.sub != nil {
		return it.sub.NextPath(ctx)
	}
	return it.next.NextPath(ctx)
}
