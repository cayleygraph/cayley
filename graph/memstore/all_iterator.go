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
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
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
	out.Int64 = *iterator.NewInt64(1, qs.nextID-1)
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
	_, ok := it.qs.revIDMap[it.Int64.Result().(int64)]
	if !ok {
		return it.Next()
	}
	return true
}

func newQuadsAllIterator(qs *QuadStore) *quadsAllIterator {
	var out quadsAllIterator
	out.Int64 = *iterator.NewInt64(1, qs.nextQuadID-1)
	out.qs = qs
	return &out
}

func (it *quadsAllIterator) Next() bool {
	out := it.Int64.Next()
	if out {
		i64 := it.Int64.Result().(int64)
		if it.qs.log[i64].DeletedBy != 0 || it.qs.log[i64].Action == graph.Delete {
			return it.Next()
		}
	}
	return out
}
