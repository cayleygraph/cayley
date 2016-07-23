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
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
)

type AllIterator struct {
	iterator.Int64
	qs *QuadStore
}

type (
	nodesAllIterator AllIterator
	quadsAllIterator AllIterator
)

func newNodesAllIterator(qs *QuadStore) *nodesAllIterator {
	var out nodesAllIterator
	out.Int64 = *iterator.NewInt64(1, qs.nextID-1, true)
	out.qs = qs
	return &out
}

// No subiterators.
func (it *nodesAllIterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *nodesAllIterator) Next() bool {
	if !it.Int64.Next() {
		return false
	}
	_, ok := it.qs.revIDMap[int64(it.Int64.Result().(iterator.Int64Node))]
	if !ok {
		return it.Next()
	}
	return true
}

func (it *nodesAllIterator) Err() error {
	return nil
}

func newQuadsAllIterator(qs *QuadStore) *quadsAllIterator {
	var out quadsAllIterator
	out.Int64 = *iterator.NewInt64(1, qs.nextQuadID-1, false)
	out.qs = qs
	return &out
}

func (it *quadsAllIterator) Next() bool {
	out := it.Int64.Next()
	if out {
		i64 := int64(it.Int64.Result().(iterator.Int64Quad))
		if it.qs.log[i64].DeletedBy != 0 || it.qs.log[i64].Action == graph.Delete {
			return it.Next()
		}
	}
	return out
}

// Override Optimize from it.Int64 - it will hide our Next implementation in other cases.

func (it *nodesAllIterator) Optimize() (graph.Iterator, bool) { return it, false }
func (it *quadsAllIterator) Optimize() (graph.Iterator, bool) { return it, false }

var _ graph.Iterator = &nodesAllIterator{}
var _ graph.Iterator = &quadsAllIterator{}
