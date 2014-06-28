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

func (ts *TripleStore) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	switch it.Type() {
	case "linksto":
		return ts.optimizeLinksTo(it.(*graph.LinksToIterator))

	}
	return it, false
}

func (ts *TripleStore) optimizeLinksTo(it *graph.LinksToIterator) (graph.Iterator, bool) {
	l := it.GetSubIterators()
	if l.Len() != 1 {
		return it, false
	}
	primaryIt := l.Front().Value.(graph.Iterator)
	if primaryIt.Type() == "fixed" {
		size, _ := primaryIt.Size()
		if size == 1 {
			val, ok := primaryIt.Next()
			if !ok {
				panic("Sizes lie")
			}
			newIt := ts.GetTripleIterator(it.Direction(), val)
			newIt.CopyTagsFrom(it)
			for _, tag := range primaryIt.Tags() {
				newIt.AddFixedTag(tag, val)
			}
			return newIt, true
		}
	}
	it.Close()
	return it, false
}
