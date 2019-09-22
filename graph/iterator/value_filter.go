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

import (
	"context"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"
)

var _ graph.IteratorFuture = &ValueFilter{}

type ValueFilter struct {
	it *valueFilter
	graph.Iterator
}

type ValueFilterFunc func(quad.Value) (bool, error)

func NewValueFilter(qs graph.Namer, sub graph.Iterator, filter ValueFilterFunc) *ValueFilter {
	it := &ValueFilter{
		it: newValueFilter(qs, graph.AsShape(sub), filter),
	}
	it.Iterator = graph.NewLegacy(it.it, it)
	return it
}

func (it *ValueFilter) AsShape() graph.IteratorShape {
	it.Close()
	return it.it
}

var _ graph.IteratorShapeCompat = (*valueFilter)(nil)

type valueFilter struct {
	sub    graph.IteratorShape
	filter ValueFilterFunc
	qs     graph.Namer
}

func newValueFilter(qs graph.Namer, sub graph.IteratorShape, filter ValueFilterFunc) *valueFilter {
	return &valueFilter{
		sub:    sub,
		qs:     qs,
		filter: filter,
	}
}

func (it *valueFilter) Iterate() graph.Scanner {
	return newValueFilterNext(it.qs, it.sub.Iterate(), it.filter)
}

func (it *valueFilter) Lookup() graph.Index {
	return newValueFilterContains(it.qs, it.sub.Lookup(), it.filter)
}

func (it *valueFilter) AsLegacy() graph.Iterator {
	it2 := &ValueFilter{it: it}
	it2.Iterator = graph.NewLegacy(it, it2)
	return it2
}

func (it *valueFilter) SubIterators() []graph.IteratorShape {
	return []graph.IteratorShape{it.sub}
}

func (it *valueFilter) String() string {
	return "ValueFilter"
}

// There's nothing to optimize, locally, for a value-comparison iterator.
// Replace the underlying iterator if need be.
// potentially replace it.
func (it *valueFilter) Optimize(ctx context.Context) (graph.IteratorShape, bool) {
	newSub, changed := it.sub.Optimize(ctx)
	if changed {
		it.sub = newSub
	}
	return it, true
}

// We're only as expensive as our subiterator.
// Again, optimized value comparison iterators should do better.
func (it *valueFilter) Stats(ctx context.Context) (graph.IteratorCosts, error) {
	st, err := it.sub.Stats(ctx)
	st.Size.Size = st.Size.Size/2 + 1
	st.Size.Exact = false
	return st, err
}

type valueFilterNext struct {
	sub    graph.Scanner
	filter ValueFilterFunc
	qs     graph.Namer
	result graph.Ref
	err    error
}

func newValueFilterNext(qs graph.Namer, sub graph.Scanner, filter ValueFilterFunc) *valueFilterNext {
	return &valueFilterNext{
		sub:    sub,
		qs:     qs,
		filter: filter,
	}
}

func (it *valueFilterNext) doFilter(val graph.Ref) bool {
	qval := it.qs.NameOf(val)
	ok, err := it.filter(qval)
	if err != nil {
		it.err = err
	}
	return ok
}

func (it *valueFilterNext) Close() error {
	return it.sub.Close()
}

func (it *valueFilterNext) Next(ctx context.Context) bool {
	for it.sub.Next(ctx) {
		val := it.sub.Result()
		if it.doFilter(val) {
			it.result = val
			return true
		}
	}
	it.err = it.sub.Err()
	return false
}

func (it *valueFilterNext) Err() error {
	return it.err
}

func (it *valueFilterNext) Result() graph.Ref {
	return it.result
}

func (it *valueFilterNext) NextPath(ctx context.Context) bool {
	return it.sub.NextPath(ctx)
}

// If we failed the check, then the subiterator should not contribute to the result
// set. Otherwise, go ahead and tag it.
func (it *valueFilterNext) TagResults(dst map[string]graph.Ref) {
	it.sub.TagResults(dst)
}

func (it *valueFilterNext) String() string {
	return "ValueFilterNext"
}

type valueFilterContains struct {
	sub    graph.Index
	filter ValueFilterFunc
	qs     graph.Namer
	result graph.Ref
	err    error
}

func newValueFilterContains(qs graph.Namer, sub graph.Index, filter ValueFilterFunc) *valueFilterContains {
	return &valueFilterContains{
		sub:    sub,
		qs:     qs,
		filter: filter,
	}
}

func (it *valueFilterContains) doFilter(val graph.Ref) bool {
	qval := it.qs.NameOf(val)
	ok, err := it.filter(qval)
	if err != nil {
		it.err = err
	}
	return ok
}

func (it *valueFilterContains) Close() error {
	return it.sub.Close()
}

func (it *valueFilterContains) Err() error {
	return it.err
}

func (it *valueFilterContains) Result() graph.Ref {
	return it.result
}

func (it *valueFilterContains) NextPath(ctx context.Context) bool {
	return it.sub.NextPath(ctx)
}

func (it *valueFilterContains) Contains(ctx context.Context, val graph.Ref) bool {
	if !it.doFilter(val) {
		return false
	}
	ok := it.sub.Contains(ctx, val)
	if !ok {
		it.err = it.sub.Err()
	}
	return ok
}

// If we failed the check, then the subiterator should not contribute to the result
// set. Otherwise, go ahead and tag it.
func (it *valueFilterContains) TagResults(dst map[string]graph.Ref) {
	it.sub.TagResults(dst)
}

func (it *valueFilterContains) String() string {
	return "ValueFilterContains"
}
