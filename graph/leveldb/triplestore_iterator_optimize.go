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

package leveldb

import (
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
)

func (ts *TripleStore) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	switch it.Type() {
	case "linksto":
		return ts.optimizeLinksTo(it.(*iterator.LinksTo))

	}
	return it, false
}

func (ts *TripleStore) optimizeLinksTo(it *iterator.LinksTo) (graph.Iterator, bool) {
	subs := it.GetSubIterators()
	if len(subs) != 1 {
		return it, false
	}
	primary := subs[0]
	if primary.Type() == "fixed" {
		size, _ := primary.Size()
		if size == 1 {
			val, ok := primary.Next()
			if !ok {
				panic("Sizes lie")
			}
			newIt := ts.GetTripleIterator(it.Direction(), val)
			newIt.CopyTagsFrom(it)
			for _, tag := range primary.Tags() {
				newIt.AddFixedTag(tag, val)
			}
			it.Close()
			return newIt, true
		}
	}
	return it, false
}
