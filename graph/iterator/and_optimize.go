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

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
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
// Contains() its subiterators. For SQL fans, this is equivalent to JOIN.
func (it *And) Optimize() (graph.Iterator, bool) {
	// First, let's get the slice of iterators, in order (first one is Next()ed,
	// the rest are Contains()ed)
	old := it.subIterators()

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
	if out := optimizeReplacement(its); out != nil && len(it.opt) == 0 {
		// ...Close everyone except `out`, our replacement...
		closeIteratorList(its, out)
		// ...And return it.
		return out, true
	}

	// And now, without changing any of the iterators, we reorder them. it_list is
	// now a permutation of itself, but the contents are unchanged.
	its = optimizeOrder(its)

	its = materializeIts(its)

	// Okay! At this point we have an optimized order.

	// The easiest thing to do at this point is merely to create a new And iterator
	// and replace ourselves with our (reordered, optimized) clone.
	newAnd := NewAnd()

	// Add the subiterators in order.
	for _, sub := range its {
		newAnd.AddSubIterator(sub)
	}
	opt := optimizeSubIterators(it.opt)
	closeIteratorList(it.opt, nil)
	for _, sub := range opt {
		newAnd.AddOptionalIterator(sub)
	}

	newAnd.optimizeContains()
	if clog.V(3) {
		clog.Infof("%v become %v", it.UID(), newAnd.UID())
	}
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
func optimizeReplacement(its []graph.Iterator) graph.Iterator {
	// If we were created with no SubIterators, we're as good as Null.
	if len(its) == 0 {
		return NewNull()
	}
	if len(its) == 1 {
		// When there's only one iterator, there's only one choice.
		return its[0]
	}
	// If any of our subiterators, post-optimization, are also Null, then
	// there's no point in continuing the branch, we will have no results
	// and we are null as well.
	if hasAnyNullIterators(its) {
		return NewNull()
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
		// graph.Optional or graph.Not. Separate them out and tack them on at the end.
		bad      []graph.Iterator
		best     graph.Iterator
		bestCost = int64(1 << 62)
	)

	// Find the iterator with the projected "best" total cost.
	// Total cost is defined as The Next()ed iterator's cost to Next() out
	// all of it's contents, and to Contains() each of those against everyone
	// else.
	for _, root := range its {
		rootStats := root.Stats()
		cost := rootStats.NextCost
		for _, f := range its {
			if f == root {
				continue
			}
			stats := f.Stats()
			cost += stats.ContainsCost * (1 + (rootStats.Size / (stats.Size + 1)))
		}
		cost *= rootStats.Size
		if clog.V(3) {
			clog.Infof("And: Root: %v Total Cost: %v Best: %v", root.UID(), cost, bestCost)
		}
		if cost < bestCost {
			best = root
			bestCost = cost
		}
	}
	if clog.V(3) {
		clog.Infof("And: Choosing: %v Best: %v", best.UID(), bestCost)
	}

	// TODO(barakmich): Optimization of order need not stop here. Picking a smart
	// Contains() order based on probability of getting a false Contains() first is
	// useful (fail faster).

	var out []graph.Iterator
	// Put the best iterator (the one we wish to Next()) at the front...
	if best != nil {
		out = append(out, best)
	}

	// ... push everyone else after...
	for _, it := range its {
		if it != best {
			out = append(out, it)
		}
	}

	// ...and finally, the difficult children on the end.
	return append(out, bad...)
}

type byCost []graph.Iterator

func (c byCost) Len() int           { return len(c) }
func (c byCost) Less(i, j int) bool { return c[i].Stats().ContainsCost < c[j].Stats().ContainsCost }
func (c byCost) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

// optimizeContains() creates an alternate check list, containing the same contents
// but with a new ordering, however it wishes.
func (it *And) optimizeContains() {
	// GetSubIterators allocates, so this is currently safe.
	// TODO(kortschak) Reuse it.checkList if possible.
	// This involves providing GetSubIterators with a slice to fill.
	// Generally this is a worthwhile thing to do in other places as well.
	it.checkList = it.subIterators()
	sort.Sort(byCost(it.checkList))
}

// optimizeSubIterators(l) takes a list of iterators and calls Optimize() on all
// of them. It returns two lists -- the first contains the same list as l, where
// any replacements are made by Optimize() and the second contains the originals
// which were replaced.
func optimizeSubIterators(its []graph.Iterator) []graph.Iterator {
	out := make([]graph.Iterator, 0, len(its))
	for _, it := range its {
		o, changed := it.Optimize()
		if !changed {
			o = it.Clone()
		}
		out = append(out, o)
	}
	return out
}

// Check a list of iterators for any Null iterators.
func hasAnyNullIterators(its []graph.Iterator) bool {
	for _, it := range its {
		if it.Type() == graph.Null {
			return true
		}
	}
	return false
}

// There are two "not-useful" iterators -- namely graph.Null which returns
// nothing, and graph.All which returns everything. Particularly, we want
// to see if we're intersecting with a bunch of graph.All iterators, and,
// if we are, then we have only one useful iterator.
//
// We already checked for hasAnyNullIteratators() -- so now we're considering
// All iterators.
func hasOneUsefulIterator(its []graph.Iterator) graph.Iterator {
	usefulCount := 0
	var usefulIt graph.Iterator
	for _, it := range its {
		switch it.Type() {
		case graph.All:
			continue
		default:
			usefulCount++
			usefulIt = it
		}
	}

	if usefulCount == 1 {
		return usefulIt
	}
	if usefulCount == 0 {
		// It's full of All iterators. We can safely return one of them.
		return its[0]
	}
	return nil
}

func materializeIts(its []graph.Iterator) []graph.Iterator {
	var out []graph.Iterator

	allStats, stats := getStatsForSlice(its, nil)
	out = append(out, its[0])
	for i, it := range its[1:] {
		st := stats[i+1]
		if st.Size*st.NextCost < (st.ContainsCost * (1 + (st.Size / (allStats.Size + 1)))) {
			if graph.Height(it, graph.Materialize) > 10 {
				out = append(out, NewMaterialize(it))
				continue
			}
		}
		out = append(out, it)
	}
	return out
}

func getStatsForSlice(its, opt []graph.Iterator) (graph.IteratorStats, []graph.IteratorStats) {
	if len(its) == 0 {
		return graph.IteratorStats{}, nil
	}

	arr := make([]graph.IteratorStats, 0, len(its))

	primaryStats := its[0].Stats()
	arr = append(arr, primaryStats)

	containsCost := primaryStats.ContainsCost
	nextCost := primaryStats.NextCost
	size := primaryStats.Size
	exact := primaryStats.ExactSize

	for _, sub := range its[1:] {
		stats := sub.Stats()
		arr = append(arr, stats)
		nextCost += stats.ContainsCost * (1 + (primaryStats.Size / (stats.Size + 1)))
		containsCost += stats.ContainsCost
		if size > stats.Size {
			size = stats.Size
			exact = stats.ExactSize
		}
	}
	for _, sub := range opt {
		stats := sub.Stats()
		nextCost += stats.ContainsCost * (1 + (primaryStats.Size / (stats.Size + 1)))
		containsCost += stats.ContainsCost
	}
	return graph.IteratorStats{
		ContainsCost: containsCost,
		NextCost:     nextCost,
		Size:         size,
		ExactSize:    exact,
	}, arr
}

// and.Stats() lives here in and-iterator-optimize.go because it may
// in the future return different statistics based on how it is optimized.
// For now, however, it's pretty static.
func (it *And) Stats() graph.IteratorStats {
	stats, _ := getStatsForSlice(it.subIterators(), it.opt)
	stats.Next = it.runstats.Next
	stats.Contains = it.runstats.Contains
	return stats
}
