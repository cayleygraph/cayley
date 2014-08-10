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
	ts *TripleStore
}

type NodesAllIterator AllIterator
type QuadsAllIterator AllIterator

func NewMemstoreNodesAllIterator(ts *TripleStore) *NodesAllIterator {
	var out NodesAllIterator
	out.Int64 = *iterator.NewInt64(1, ts.idCounter-1)
	out.ts = ts
	return &out
}

// No subiterators.
func (nit *NodesAllIterator) SubIterators() []graph.Iterator {
	return nil
}

func (nit *NodesAllIterator) Next() bool {
	if !nit.Int64.Next() {
		return false
	}
	_, ok := nit.ts.revIdMap[nit.Int64.Result().(int64)]
	if !ok {
		return nit.Next()
	}
	return true
}

func NewMemstoreQuadsAllIterator(ts *TripleStore) *QuadsAllIterator {
	var out QuadsAllIterator
	out.Int64 = *iterator.NewInt64(1, ts.quadIdCounter-1)
	out.ts = ts
	return &out
}

func (qit *QuadsAllIterator) Next() bool {
	out := qit.Int64.Next()
	if out {
		i64 := qit.Int64.Result().(int64)
		if qit.ts.log[i64].DeletedBy != 0 || qit.ts.log[i64].Action == graph.Delete {
			return qit.Next()
		}
	}
	return out
}
