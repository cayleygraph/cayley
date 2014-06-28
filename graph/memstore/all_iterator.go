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
)

type AllIterator struct {
	graph.Int64AllIterator
	ts *TripleStore
}

func NewMemstoreAllIterator(ts *TripleStore) *AllIterator {
	var out AllIterator
	out.Int64AllIterator = *graph.NewInt64AllIterator(1, ts.idCounter-1)
	out.ts = ts
	return &out
}

func (it *AllIterator) Next() (graph.TSVal, bool) {
	next, out := it.Int64AllIterator.Next()
	if !out {
		return next, out
	}
	i64 := next.(int64)
	_, ok := it.ts.revIdMap[i64]
	if !ok {
		return it.Next()
	}
	it.Last = next
	return next, out
}
