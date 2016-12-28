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

package bolt2

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
)

type AllIterator struct {
	nodes   bool
	id      uint64
	horizon int64
	tags    graph.Tagger
	qs      *QuadStore
	err     error
	uid     uint64
}

var _ graph.Iterator = &AllIterator{}

func NewAllIterator(nodes bool, qs *QuadStore) *AllIterator {
	return &AllIterator{
		nodes:   nodes,
		qs:      qs,
		horizon: qs.horizon,
		uid:     iterator.NextUID(),
	}
}

func (it *AllIterator) UID() uint64 {
	return it.uid
}

func (it *AllIterator) Reset() {
	it.id = 0
}

func (it *AllIterator) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *AllIterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *AllIterator) Clone() graph.Iterator {
	out := NewAllIterator(it.nodes, it.qs)
	out.tags.CopyFrom(it)
	return out
}

func (it *AllIterator) Close() error {
	return nil
}

func (it *AllIterator) Err() error {
	return it.err
}

func (it *AllIterator) Result() graph.Value {
	if it.id > uint64(it.horizon) {
		return nil
	}
	return Int64Value(it.id)
}

// No subiterators.
func (it *AllIterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *AllIterator) Next() bool {
	for {
		it.id++
		if it.id > uint64(it.horizon) {
			return false
		}
		p, ok := it.qs.getPrimitive(Int64Value(it.id))
		if !ok {
			return false
		}
		if p.IsNode() && it.nodes {
			return true
		}
		if !p.IsNode() && !it.nodes {
			return true
		}
	}
}

func (it *AllIterator) NextPath() bool {
	return false
}

func (it *AllIterator) Contains(v graph.Value) bool {
	it.id = uint64(v.(Int64Value))
	return it.id <= uint64(it.horizon)
}

func (it *AllIterator) Size() (int64, bool) {
	return it.horizon, false
}

func (it *AllIterator) Describe() graph.Description {
	size, _ := it.Size()
	return graph.Description{
		UID:       it.UID(),
		Type:      it.Type(),
		Tags:      it.tags.Tags(),
		Size:      size,
		Direction: quad.Any,
	}
}

func (it *AllIterator) Type() graph.Type { return graph.All }
func (it *AllIterator) Sorted() bool     { return false }

func (it *AllIterator) Optimize() (graph.Iterator, bool) {
	return it, false
}

func (it *AllIterator) Stats() graph.IteratorStats {
	s, exact := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     2,
		Size:         s,
		ExactSize:    exact,
	}
}
