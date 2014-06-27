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
	"container/list"
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
	direction string
	nextIt    Iterator
}

// Construct a new LinksTo iterator around a direction and a subiterator of
// nodes.
func NewLinksToIterator(ts TripleStore, it Iterator, dir string) *LinksToIterator {
	var lto LinksToIterator
	BaseIteratorInit(&lto.BaseIterator)
	lto.ts = ts
	lto.primaryIt = it
	lto.direction = dir
	lto.nextIt = &NullIterator{}
	return &lto
}

func (l *LinksToIterator) Reset() {
	l.primaryIt.Reset()
	if l.nextIt != nil {
		l.nextIt.Close()
	}
	l.nextIt = &NullIterator{}
}

func (l *LinksToIterator) Clone() Iterator {
	out := NewLinksToIterator(l.ts, l.primaryIt.Clone(), l.direction)
	out.CopyTagsFrom(l)
	return out
}

// Return the direction under consideration.
func (l *LinksToIterator) Direction() string { return l.direction }

// Tag these results, and our subiterator's results.
func (l *LinksToIterator) TagResults(out *map[string]TSVal) {
	l.BaseIterator.TagResults(out)
	l.primaryIt.TagResults(out)
}

// DEPRECATED
func (l *LinksToIterator) GetResultTree() *ResultTree {
	tree := NewResultTree(l.LastResult())
	tree.AddSubtree(l.primaryIt.GetResultTree())
	return tree
}

// Print the iterator.
func (l *LinksToIterator) DebugString(indent int) string {
	return fmt.Sprintf("%s(%s %d direction:%s\n%s)",
		strings.Repeat(" ", indent),
		l.Type(), l.GetUid(), l.direction, l.primaryIt.DebugString(indent+4))
}

// If it checks in the right direction for the subiterator, it is a valid link
// for the LinksTo.
func (l *LinksToIterator) Check(val TSVal) bool {
	CheckLogIn(l, val)
	node := l.ts.GetTripleDirection(val, l.direction)
	if l.primaryIt.Check(node) {
		l.Last = val
		return CheckLogOut(l, val, true)
	}
	return CheckLogOut(l, val, false)
}

// Return a list containing only our subiterator.
func (lto *LinksToIterator) GetSubIterators() *list.List {
	l := list.New()
	l.PushBack(lto.primaryIt)
	return l
}

// Optimize the LinksTo, by replacing it if it can be.
func (lto *LinksToIterator) Optimize() (Iterator, bool) {
	newPrimary, changed := lto.primaryIt.Optimize()
	if changed {
		lto.primaryIt = newPrimary
		if lto.primaryIt.Type() == "null" {
			lto.nextIt.Close()
			return lto.primaryIt, true
		}
	}
	// Ask the TripleStore if we can be replaced. Often times, this is a great
	// optimization opportunity (there's a fixed iterator underneath us, for
	// example).
	newReplacement, hasOne := lto.ts.OptimizeIterator(lto)
	if hasOne {
		lto.Close()
		return newReplacement, true
	}
	return lto, false
}

// Next()ing a LinksTo operates as described above.
func (l *LinksToIterator) Next() (TSVal, bool) {
	NextLogIn(l)
	val, ok := l.nextIt.Next()
	if !ok {
		// Subiterator is empty, get another one
		candidate, ok := l.primaryIt.Next()
		if !ok {
			// We're out of nodes in our subiterator, so we're done as well.
			return NextLogOut(l, 0, false)
		}
		l.nextIt.Close()
		l.nextIt = l.ts.GetTripleIterator(l.direction, candidate)
		// Recurse -- return the first in the next set.
		return l.Next()
	}
	l.Last = val
	return NextLogOut(l, val, ok)
}

// Close our subiterators.
func (l *LinksToIterator) Close() {
	l.nextIt.Close()
	l.primaryIt.Close()
}

// We won't ever have a new result, but our subiterators might.
func (l *LinksToIterator) NextResult() bool {
	return l.primaryIt.NextResult()
}

// Register the LinksTo.
func (l *LinksToIterator) Type() string { return "linksto" }

// Return a guess as to how big or costly it is to next the iterator.
func (l *LinksToIterator) GetStats() *IteratorStats {
	subitStats := l.primaryIt.GetStats()
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
