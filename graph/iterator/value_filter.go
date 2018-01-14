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
	"github.com/cayleygraph/cayley/quad"
)

var _ graph.Iterator = &ValueFilter{}

type ValueFilter struct {
	uid    uint64
	sub    graph.Iterator
	filter ValueFilterFunc
	qs     graph.QuadStore
	result graph.Value
	err    error
}

type ValueFilterFunc func(quad.Value) (bool, error)

func NewValueFilter(qs graph.QuadStore, sub graph.Iterator, filter ValueFilterFunc) *ValueFilter {
	return &ValueFilter{
		uid:    NextUID(),
		sub:    sub,
		qs:     qs,
		filter: filter,
	}
}

func (it *ValueFilter) UID() uint64 {
	return it.uid
}

func (it *ValueFilter) doFilter(val graph.Value) bool {
	qval := it.qs.NameOf(val)
	ok, err := it.filter(qval)
	if err != nil {
		it.err = err
	}
	return ok
}

func (it *ValueFilter) Close() error {
	return it.sub.Close()
}

func (it *ValueFilter) Reset() {
	it.sub.Reset()
	it.err = nil
	it.result = nil
}

func (it *ValueFilter) Clone() graph.Iterator {
	return NewValueFilter(it.qs, it.sub.Clone(), it.filter)
}

func (it *ValueFilter) Next(ctx context.Context) bool {
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

func (it *ValueFilter) Err() error {
	return it.err
}

func (it *ValueFilter) Result() graph.Value {
	return it.result
}

func (it *ValueFilter) NextPath(ctx context.Context) bool {
	for {
		hasNext := it.sub.NextPath(ctx)
		if !hasNext {
			it.err = it.sub.Err()
			return false
		}
		if it.doFilter(it.sub.Result()) {
			break
		}
	}
	it.result = it.sub.Result()
	return true
}

func (it *ValueFilter) SubIterators() []graph.Iterator {
	return []graph.Iterator{it.sub}
}

func (it *ValueFilter) Contains(ctx context.Context, val graph.Value) bool {
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
func (it *ValueFilter) TagResults(dst map[string]graph.Value) {
	it.sub.TagResults(dst)
}

// Registers the value-comparison iterator.
func (it *ValueFilter) Type() graph.Type { return graph.Comparison }

func (it *ValueFilter) String() string {
	return "ValueFilter"
}

// There's nothing to optimize, locally, for a value-comparison iterator.
// Replace the underlying iterator if need be.
// potentially replace it.
func (it *ValueFilter) Optimize() (graph.Iterator, bool) {
	newSub, changed := it.sub.Optimize()
	if changed {
		it.sub.Close()
		it.sub = newSub
	}
	return it, false
}

// We're only as expensive as our subiterator.
// Again, optimized value comparison iterators should do better.
func (it *ValueFilter) Stats() graph.IteratorStats {
	return it.sub.Stats()
}

func (it *ValueFilter) Size() (int64, bool) {
	sz, _ := it.sub.Size()
	return sz / 2, false
}
