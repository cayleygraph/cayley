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

package graph

// Defines one of the base iterators, the LinksTo iterator. A LinksTo takes a
// subiterator of nodes, and contains an iteration of links which "link to"
// those nodes in a given direction.
//
// Next()ing a LinksTo is straightforward -- iterate through all links to //
// things in the subiterator, and then advance the subiterator, and do it again.
// LinksTo is therefore sensitive to growing with a fanout. (A small-sized
// subiterator could cause LinksTo to be large).
//
// Check()ing a LinksTo means, given a link, take the direction we care about
// and check if it's in our subiterator. Checking is therefore fairly cheap, and
// similar to checking the subiterator alone.
//
// Can be seen as the dual of the HasA iterator.

import (
	"fmt"
	"strings"
)

// A LinksTo has a reference back to the TripleStore (to create the iterators
// for each node) the subiterator, and the direction the iterator comes from.
// `next_it` is the tempoarary iterator held per result in `primary_it`.
type LinksToIterator struct {
	BaseIterator
	ts        TripleStore
	primaryIt Iterator
	dir       Direction
	nextIt    Iterator
}

// Construct a new LinksTo iterator around a direction and a subiterator of
// nodes.
func NewLinksToIterator(ts TripleStore, it Iterator, d Direction) *LinksToIterator {
	var lto LinksToIterator
	BaseIteratorInit(&lto.BaseIterator)
	lto.ts = ts
	lto.primaryIt = it
	lto.dir = d
	lto.nextIt = &NullIterator{}
	return &lto
}

func (it *LinksToIterator) Reset() {
	it.primaryIt.Reset()
	if it.nextIt != nil {
		it.nextIt.Close()
	}
	it.nextIt = &NullIterator{}
}

func (it *LinksToIterator) Clone() Iterator {
	out := NewLinksToIterator(it.ts, it.primaryIt.Clone(), it.dir)
	out.CopyTagsFrom(it)
	return out
}

// Return the direction under consideration.
func (it *LinksToIterator) Direction() Direction { return it.dir }

// Tag these results, and our subiterator's results.
func (it *LinksToIterator) TagResults(out *map[string]TSVal) {
	it.BaseIterator.TagResults(out)
	it.primaryIt.TagResults(out)
}

// DEPRECATED
func (it *LinksToIterator) GetResultTree() *ResultTree {
	tree := NewResultTree(it.LastResult())
	tree.AddSubtree(it.primaryIt.GetResultTree())
	return tree
}

// Print the iterator.
func (it *LinksToIterator) DebugString(indent int) string {
	return fmt.Sprintf("%s(%s %d direction:%s\n%s)",
		strings.Repeat(" ", indent),
		it.Type(), it.GetUid(), it.dir, it.primaryIt.DebugString(indent+4))
}

// If it checks in the right direction for the subiterator, it is a valid link
// for the LinksTo.
func (it *LinksToIterator) Check(val TSVal) bool {
	CheckLogIn(it, val)
	node := it.ts.GetTripleDirection(val, it.dir)
	if it.primaryIt.Check(node) {
		it.Last = val
		return CheckLogOut(it, val, true)
	}
	return CheckLogOut(it, val, false)
}

// Return a list containing only our subiterator.
func (it *LinksToIterator) GetSubIterators() []Iterator {
	return []Iterator{it.primaryIt}
}

// Optimize the LinksTo, by replacing it if it can be.
func (it *LinksToIterator) Optimize() (Iterator, bool) {
	newPrimary, changed := it.primaryIt.Optimize()
	if changed {
		it.primaryIt = newPrimary
		if it.primaryIt.Type() == "null" {
			it.nextIt.Close()
			return it.primaryIt, true
		}
	}
	// Ask the TripleStore if we can be replaced. Often times, this is a great
	// optimization opportunity (there's a fixed iterator underneath us, for
	// example).
	newReplacement, hasOne := it.ts.OptimizeIterator(it)
	if hasOne {
		it.Close()
		return newReplacement, true
	}
	return it, false
}

// Next()ing a LinksTo operates as described above.
func (it *LinksToIterator) Next() (TSVal, bool) {
	NextLogIn(it)
	val, ok := it.nextIt.Next()
	if !ok {
		// Subiterator is empty, get another one
		candidate, ok := it.primaryIt.Next()
		if !ok {
			// We're out of nodes in our subiterator, so we're done as well.
			return NextLogOut(it, 0, false)
		}
		it.nextIt.Close()
		it.nextIt = it.ts.GetTripleIterator(it.dir, candidate)
		// Recurse -- return the first in the next set.
		return it.Next()
	}
	it.Last = val
	return NextLogOut(it, val, ok)
}

// Close our subiterators.
func (it *LinksToIterator) Close() {
	it.nextIt.Close()
	it.primaryIt.Close()
}

// We won't ever have a new result, but our subiterators might.
func (it *LinksToIterator) NextResult() bool {
	return it.primaryIt.NextResult()
}

// Register the LinksTo.
func (it *LinksToIterator) Type() string { return "linksto" }

// Return a guess as to how big or costly it is to next the iterator.
func (it *LinksToIterator) GetStats() *IteratorStats {
	subitStats := it.primaryIt.GetStats()
	// TODO(barakmich): These should really come from the triplestore itself
	fanoutFactor := int64(20)
	checkConstant := int64(1)
	nextConstant := int64(2)
	return &IteratorStats{
		NextCost:  nextConstant + subitStats.NextCost,
		CheckCost: checkConstant + subitStats.CheckCost,
		Size:      fanoutFactor * subitStats.Size,
	}
}
