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

package iterator

import (
	"sort"

	"github.com/google/cayley/graph"
)

// Perhaps the most tricky file in this entire module. Really a method on the
// And, but important enough to deserve its own file.
//
// Calling Optimize() on an And iterator, like any iterator, requires that we
// preserve the underlying meaning. However, the And has many choices, namely,
// which one of it's subiterators will be the branch that does the Next()ing,
// and which ordering of the remaining iterators is the most efficient. In
// short, this is where a lot of the query optimization happens, and there are
// many wins to be had here, as well as many bad bugs. The worst class of bug
// changes the meaning of the query. The second worst class makes things really
// slow.
//
// The good news is this: If Optimize() is never called (turned off, perhaps) we can
// be sure the results are as good as the query language called for.
//
// In short, tread lightly.

// Optimizes the And, by picking the most efficient way to Next() and
// Check() its subiterators. For SQL fans, this is equivalent to JOIN.
func (it *And) Optimize() (graph.Iterator, bool) {
	// First, let's get the slice of iterators, in order (first one is Next()ed,
	// the rest are Check()ed)
	old := it.GetSubIterators()

	// And call Optimize() on our subtree, replacing each one in the order we
	// found them. it_list is the newly optimized versions of these, and changed
	// is another list, of only the ones that have returned replacements and
	// changed.
	its := optimizeSubIterators(old)

	// Close the replaced iterators (they ought to close themselves, but Close()
	// is idempotent, so this just protects against any machinations).
	closeIteratorList(old, nil)

	// If we can find only one subiterator which is equivalent to this whole and,
	// we can replace the And...
	out := it.optimizeReplacement(its)
	if out != nil {
		// ...Move the tags to the replacement...
		moveTagsTo(out, it)
		// ...Close everyone except `out`, our replacement...
		closeIteratorList(its, out)
		// ...And return it.
		return out, true
	}

	// And now, without changing any of the iterators, we reorder them. it_list is
	// now a permutation of itself, but the contents are unchanged.
	its = optimizeOrder(its)

	// Okay! At this point we have an optimized order.

	// The easiest thing to do at this point is merely to create a new And iterator
	// and replace ourselves with our (reordered, optimized) clone.
	newAnd := NewAnd()

	// Add the subiterators in order.
	for _, sub := range its {
		newAnd.AddSubIterator(sub)
	}

	// Move the tags hanging on us (like any good replacement).
	newAnd.CopyTagsFrom(it)

	newAnd.optimizeCheck()

	// And close ourselves but not our subiterators -- some may still be alive in
	// the new And (they were unchanged upon calling Optimize() on them, at the
	// start).
	it.cleanUp()
	return newAnd, true
}

// Closes a list of iterators, except the one passed in `except`. Closes all
// of the iterators in the list if `except` is nil.
func closeIteratorList(its []graph.Iterator, except graph.Iterator) {
	for _, it := range its {
		if it != except {
			it.Close()
		}
	}
}

// Find if there is a single subiterator which is a valid replacement for this
// And.
func (_ *And) optimizeReplacement(its []graph.Iterator) graph.Iterator {
	// If we were created with no SubIterators, we're as good as Null.
	if len(its) == 0 {
		return &Null{}
	}
	if len(its) == 1 {
		// When there's only one iterator, there's only one choice.
		return its[0]
	}
	// If any of our subiterators, post-optimization, are also Null, then
	// there's no point in continuing the branch, we will have no results
	// and we are null as well.
	if hasAnyNullIterators(its) {
		return &Null{}
	}

	// If we have one useful iterator, use that.
	it := hasOneUsefulIterator(its)
	if it != nil {
		return it
	}
	return nil
}

// optimizeOrder(l) takes a list and returns a list, containing the same contents
// but with a new ordering, however it wishes.
func optimizeOrder(its []graph.Iterator) []graph.Iterator {
	var (
		// bad contains iterators that can't be (efficiently) nexted, such as
		// "optional" or "not". Separate them out and tack them on at the end.
		out, bad []graph.Iterator
		best     graph.Iterator
		bestCost = int64(1 << 62)
	)

	// Find the iterator with the projected "best" total cost.
	// Total cost is defined as The Next()ed iterator's cost to Next() out
	// all of it's contents, and to Check() each of those against everyone
	// else.
	for _, it := range its {
		if !it.Nextable() {
			bad = append(bad, it)
			continue
		}
		rootStats := it.GetStats()
		cost := rootStats.NextCost
		for _, f := range its {
			if !f.Nextable() {
				continue
			}
			if f == it {
				continue
			}
			stats := f.GetStats()
			cost += stats.CheckCost
		}
		cost *= rootStats.Size
		if cost < bestCost {
			best = it
			bestCost = cost
		}
	}

	// TODO(barakmich): Optimization of order need not stop here. Picking a smart
	// Check() order based on probability of getting a false Check() first is
	// useful (fail faster).

	// Put the best iterator (the one we wish to Next()) at the front...
	out = append(out, best)

	// ... push everyone else after...
	for _, it := range its {
		if !it.Nextable() {
			continue
		}
		if it != best {
			out = append(out, it)
		}
	}

	// ...and finally, the difficult children on the end.
	return append(out, bad...)
}

type byCost []graph.Iterator

func (c byCost) Len() int           { return len(c) }
func (c byCost) Less(i, j int) bool { return c[i].GetStats().CheckCost < c[j].GetStats().CheckCost }
func (c byCost) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

// optimizeCheck(l) creates an alternate check list, containing the same contents
// but with a new ordering, however it wishes.
func (it *And) optimizeCheck() {
	// GetSubIterators allocates, so this is currently safe.
	// TODO(kortschak) Reuse it.checkList if possible.
	// This involves providing GetSubIterators with a slice to fill.
	// Generally this is a worthwhile thing to do in other places as well.
	it.checkList = it.GetSubIterators()
	sort.Sort(byCost(it.checkList))
}

// If we're replacing ourselves by a single iterator, we need to grab the
// result tags from the iterators that, while still valid and would hold
// the same values as this and, are not going to stay.
// getSubTags() returns a map of the tags for all the subiterators.
func (it *And) getSubTags() map[string]struct{} {
	tags := make(map[string]struct{})
	for _, sub := range it.GetSubIterators() {
		for _, tag := range sub.Tags() {
			tags[tag] = struct{}{}
		}
	}
	for _, tag := range it.Tags() {
		tags[tag] = struct{}{}
	}
	return tags
}

// moveTagsTo() gets the tags for all of the src's subiterators and the
// src itself, and moves them to dst.
func moveTagsTo(dst graph.Iterator, src *And) {
	tags := src.getSubTags()
	for _, tag := range dst.Tags() {
		if _, ok := tags[tag]; ok {
			delete(tags, tag)
		}
	}
	for k := range tags {
		dst.AddTag(k)
	}
}

// optimizeSubIterators(l) takes a list of iterators and calls Optimize() on all
// of them. It returns two lists -- the first contains the same list as l, where
// any replacements are made by Optimize() and the second contains the originals
// which were replaced.
func optimizeSubIterators(its []graph.Iterator) []graph.Iterator {
	var optIts []graph.Iterator
	for _, it := range its {
		o, changed := it.Optimize()
		if changed {
			optIts = append(optIts, o)
		} else {
			optIts = append(optIts, it.Clone())
		}
	}
	return optIts
}

// Check a list of iterators for any Null iterators.
func hasAnyNullIterators(its []graph.Iterator) bool {
	for _, it := range its {
		if it.Type() == "null" {
			return true
		}
	}
	return false
}

// There are two "not-useful" iterators -- namely "null" which returns
// nothing, and "all" which returns everything. Particularly, we want
// to see if we're intersecting with a bunch of "all" iterators, and,
// if we are, then we have only one useful iterator.
func hasOneUsefulIterator(its []graph.Iterator) graph.Iterator {
	usefulCount := 0
	var usefulIt graph.Iterator
	for _, it := range its {
		switch it.Type() {
		case "null", "all":
			continue
		case "optional":
			// Optional is weird -- it's not useful, but we can't optimize
			// away from it. Therefore, we skip this optimization
			// if we see one.
			return nil
		default:
			usefulCount++
			usefulIt = it
		}
	}

	if usefulCount == 1 {
		return usefulIt
	}
	return nil
}

// and.GetStats() lives here in and-iterator-optimize.go because it may
// in the future return different statistics based on how it is optimized.
// For now, however, it's pretty static.
func (it *And) GetStats() *graph.IteratorStats {
	primaryStats := it.primaryIt.GetStats()
	CheckCost := primaryStats.CheckCost
	NextCost := primaryStats.NextCost
	Size := primaryStats.Size
	for _, sub := range it.internalIterators {
		stats := sub.GetStats()
		NextCost += stats.CheckCost
		CheckCost += stats.CheckCost
		if Size > stats.Size {
			Size = stats.Size
		}
	}
	return &graph.IteratorStats{
		CheckCost: CheckCost,
		NextCost:  NextCost,
		Size:      Size,
	}

}
