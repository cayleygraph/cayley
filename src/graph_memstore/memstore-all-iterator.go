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

package graph_memstore

import (
	"github.com/google/cayley/src/graph"
)

type MemstoreAllIterator struct {
	graph.Int64AllIterator
	ts *MemTripleStore
}

func NewMemstoreAllIterator(ts *MemTripleStore) *MemstoreAllIterator {
	var out MemstoreAllIterator
	out.Int64AllIterator = *graph.NewInt64AllIterator(1, ts.idCounter-1)
	out.ts = ts
	return &out
}

func (memall *MemstoreAllIterator) Next() (graph.TSVal, bool) {
	next, out := memall.Int64AllIterator.Next()
	if !out {
		return next, out
	}
	i64 := next.(int64)
	_, ok := memall.ts.revIdMap[i64]
	if !ok {
		return memall.Next()
	}
	memall.Last = next
	return next, out
}
