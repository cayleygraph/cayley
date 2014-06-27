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

// Defines one of the base iterators, the HasA iterator. The HasA takes a
// subiterator of links, and acts as an iterator of nodes in the given
// direction. The name comes from the idea that a "link HasA subject" or a "link
// HasA predicate".
//
// HasA is weird in that it may return the same value twice if on the Next()
// path. That's okay -- in reality, it can be viewed as returning the value for
// a new triple, but to make logic much simpler, here we have the HasA.
//
// Likewise, it's important to think about Check()ing a HasA. When given a
// value to check, it means "Check all predicates that have this value for your
// direction against the subiterator." This would imply that there's more than
// one possibility for the same Check()ed value. While we could return the
// number of options, it's simpler to return one, and then call NextResult()
// enough times to enumerate the options. (In fact, one could argue that the
// raison d'etre for NextResult() is this iterator).
//
// Alternatively, can be seen as the dual of the LinksTo iterator.

import (
	"container/list"
	"fmt"
	"github.com/barakmich/glog"
	"strings"
)

// A HasaIterator consists of a reference back to the TripleStore that it references,
// a primary subiterator, a direction in which the triples for that subiterator point,
// and a temporary holder for the iterator generated on Check().
type HasaIterator struct {
	BaseIterator
	ts        TripleStore
	primaryIt Iterator
	direction string
	resultIt  Iterator
}

// Construct a new HasA iterator, given the triple subiterator, and the triple
// direction for which it stands.
func NewHasaIterator(ts TripleStore, subIt Iterator, dir string) *HasaIterator {
	var hasa HasaIterator
	BaseIteratorInit(&hasa.BaseIterator)
	hasa.ts = ts
	hasa.primaryIt = subIt
	hasa.direction = dir
	return &hasa
}

// Return our sole subiterator, in a list.List.
func (h *HasaIterator) GetSubIterators() *list.List {
	l := list.New()
	l.PushBack(h.primaryIt)
	return l
}

func (h *HasaIterator) Reset() {
	h.primaryIt.Reset()
	if h.resultIt != nil {
		h.resultIt.Close()
	}
}

func (h *HasaIterator) Clone() Iterator {
	out := NewHasaIterator(h.ts, h.primaryIt.Clone(), h.direction)
	out.CopyTagsFrom(h)
	return out
}

// Direction accessor.
func (h *HasaIterator) Direction() string { return h.direction }

// Pass the Optimize() call along to the subiterator. If it becomes Null,
// then the HasA becomes Null (there are no triples that have any directions).
func (h *HasaIterator) Optimize() (Iterator, bool) {

	newPrimary, changed := h.primaryIt.Optimize()
	if changed {
		h.primaryIt = newPrimary
		if h.primaryIt.Type() == "null" {
			return h.primaryIt, true
		}
	}
	return h, false
}

// Pass the TagResults down the chain.
func (h *HasaIterator) TagResults(out *map[string]TSVal) {
	h.BaseIterator.TagResults(out)
	h.primaryIt.TagResults(out)
}

// DEPRECATED Return results in a ResultTree.
func (h *HasaIterator) GetResultTree() *ResultTree {
	tree := NewResultTree(h.LastResult())
	tree.AddSubtree(h.primaryIt.GetResultTree())
	return tree
}

// Print some information about this iterator.
func (h *HasaIterator) DebugString(indent int) string {
	var tags string
	for _, k := range h.Tags() {
		tags += fmt.Sprintf("%s;", k)
	}
	return fmt.Sprintf("%s(%s %d tags:%s direction:%s\n%s)", strings.Repeat(" ", indent), h.Type(), h.GetUid(), tags, h.direction, h.primaryIt.DebugString(indent+4))
}

// Check a value against our internal iterator. In order to do this, we must first open a new
// iterator of "triples that have `val` in our direction", given to us by the triple store,
// and then Next() values out of that iterator and Check() them against our subiterator.
func (h *HasaIterator) Check(val TSVal) bool {
	CheckLogIn(h, val)
	if glog.V(4) {
		glog.V(4).Infoln("Id is", h.ts.GetNameFor(val))
	}
	// TODO(barakmich): Optimize this
	if h.resultIt != nil {
		h.resultIt.Close()
	}
	h.resultIt = h.ts.GetTripleIterator(h.direction, val)
	return CheckLogOut(h, val, h.GetCheckResult())
}

// GetCheckResult() is shared code between Check() and GetNextResult() -- calls next on the
// result iterator (a triple iterator based on the last checked value) and returns true if
// another match is made.
func (h *HasaIterator) GetCheckResult() bool {
	for {
		linkVal, ok := h.resultIt.Next()
		if !ok {
			break
		}
		if glog.V(4) {
			glog.V(4).Infoln("Triple is", h.ts.GetTriple(linkVal).ToString())
		}
		if h.primaryIt.Check(linkVal) {
			h.Last = h.ts.GetTripleDirection(linkVal, h.direction)
			return true
		}
	}
	return false
}

// Get the next result that matches this branch.
func (h *HasaIterator) NextResult() bool {
	// Order here is important. If the subiterator has a NextResult, then we
	// need do nothing -- there is a next result, and we shouldn't move forward.
	// However, we then need to get the next result from our last Check().
	//
	// The upshot is, the end of NextResult() bubbles up from the bottom of the
	// iterator tree up, and we need to respect that.
	if h.primaryIt.NextResult() {
		return true
	}
	return h.GetCheckResult()
}

// Get the next result from this iterator. This is simpler than Check. We have a
// subiterator we can get a value from, and we can take that resultant triple,
// pull our direction out of it, and return that.
func (h *HasaIterator) Next() (TSVal, bool) {
	NextLogIn(h)
	if h.resultIt != nil {
		h.resultIt.Close()
	}
	h.resultIt = &NullIterator{}

	tID, ok := h.primaryIt.Next()
	if !ok {
		return NextLogOut(h, 0, false)
	}
	name := h.ts.GetTriple(tID).Get(h.direction)
	val := h.ts.GetIdFor(name)
	h.Last = val
	return NextLogOut(h, val, true)
}

// GetStats() returns the statistics on the HasA iterator. This is curious. Next
// cost is easy, it's an extra call or so on top of the subiterator Next cost.
// CheckCost involves going to the TripleStore, iterating out values, and hoping
// one sticks -- potentially expensive, depending on fanout. Size, however, is
// potentially smaller. we know at worst it's the size of the subiterator, but
// if there are many repeated values, it could be much smaller in totality.
func (h *HasaIterator) GetStats() *IteratorStats {
	subitStats := h.primaryIt.GetStats()
	// TODO(barakmich): These should really come from the triplestore itself
	// and be optimized.
	faninFactor := int64(1)
	fanoutFactor := int64(30)
	nextConstant := int64(2)
	tripleConstant := int64(1)
	return &IteratorStats{
		NextCost:  tripleConstant + subitStats.NextCost,
		CheckCost: (fanoutFactor * nextConstant) * subitStats.CheckCost,
		Size:      faninFactor * subitStats.Size,
	}
}

// Close the subiterator, the result iterator (if any) and the HasA.
func (h *HasaIterator) Close() {
	if h.resultIt != nil {
		h.resultIt.Close()
	}
	h.primaryIt.Close()
}

// Register this iterator as a HasA.
func (h *HasaIterator) Type() string { return "hasa" }
