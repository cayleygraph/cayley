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
func (it *AllIterator) SubIterators() []graph.Iterator {
	return nil
}

func (nit *NodesAllIterator) Next() (graph.Value, bool) {
	next, out := nit.Int64.Next()
	if !out {
		return next, out
	}
	i64 := next.(int64)
	_, ok := nit.ts.revIdMap[i64]
	if !ok {
		return nit.Next()
	}
	return next, out
}

func NewMemstoreQuadsAllIterator(ts *TripleStore) *QuadsAllIterator {
	var out QuadsAllIterator
	out.Int64 = *iterator.NewInt64(1, ts.quadIdCounter-1)
	out.ts = ts
	return &out
}

func (qit *QuadsAllIterator) Next() (graph.Value, bool) {
	next, out := qit.Int64.Next()
	if out {
		i64 := next.(int64)
		if qit.ts.log[i64].DeletedBy != 0 || qit.ts.log[i64].Action == graph.Delete {
			return qit.Next()
		}
	}
	return next, out
}
