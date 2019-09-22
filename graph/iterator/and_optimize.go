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
	"context"
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
func (it *and) Optimize(ctx context.Context) (graph.IteratorShape, bool) {
	// First, let's get the slice of iterators, in order (first one is Next()ed,
	// the rest are Contains()ed)
	old := it.sub
	if len(old) == 0 {
		return newNull(), true
	}

	// And call Optimize() on our subtree, replacing each one in the order we
	// found them. it_list is the newly optimized versions of these, and changed
	// is another list, of only the ones that have returned replacements and
	// changed.
	its := optimizeSubIterators2(ctx, old)

	// If we can find only one subiterator which is equivalent to this whole and,
	// we can replace the And...
	if out := optimizeReplacement(its); out != nil && len(it.opt) == 0 {
		// ...And return it.
		return out, true
	}

	// And now, without changing any of the iterators, we reorder them. it_list is
	// now a permutation of itself, but the contents are unchanged.
	its = optimizeOrder(ctx, its)

	its, _ = materializeIts(ctx, its)

	// Okay! At this point we have an optimized order.

	// The easiest thing to do at this point is merely to create a new And iterator
	// and replace ourselves with our (reordered, optimized) clone.
	// Add the subiterators in order.
	newAnd := newAnd(its...)

	opt := optimizeSubIterators2(ctx, it.opt)
	for _, sub := range opt {
		newAnd.AddOptionalIterator(sub)
	}

	_ = newAnd.optimizeContains(ctx)
	if clog.V(3) {
		clog.Infof("%p become %p", it, newAnd)
	}
	return newAnd, true
}

// Find if there is a single subiterator which is a valid replacement for this
// And.
func optimizeReplacement(its []graph.IteratorShape) graph.IteratorShape {
	// If we were created with no SubIterators, we're as good as Null.
	if len(its) == 0 {
		return newNull()
	}
	if len(its) == 1 {
		// When there's only one iterator, there's only one choice.
		return its[0]
	}
	// If any of our subiterators, post-optimization, are also Null, then
	// there's no point in continuing the branch, we will have no results
	// and we are null as well.
	if hasAnyNullIterators(its) {
		return newNull()
	}
	return nil
}

// optimizeOrder(l) takes a list and returns a list, containing the same contents
// but with a new ordering, however it wishes.
func optimizeOrder(ctx context.Context, its []graph.IteratorShape) []graph.IteratorShape {
	var (
		// bad contains iterators that can't be (efficiently) nexted, such as
		// graph.Optional or graph.Not. Separate them out and tack them on at the end.
		bad      []graph.IteratorShape
		best     graph.IteratorShape
		bestCost = int64(1 << 62)
	)

	// Find the iterator with the projected "best" total cost.
	// Total cost is defined as The Next()ed iterator's cost to Next() out
	// all of it's contents, and to Contains() each of those against everyone
	// else.
	for _, root := range its {
		rootStats, _ := root.Stats(ctx)
		cost := rootStats.NextCost
		for _, f := range its {
			if f == root {
				continue
			}
			stats, _ := f.Stats(ctx)
			cost += stats.ContainsCost * (1 + (rootStats.Size.Size / (stats.Size.Size + 1)))
		}
		cost *= rootStats.Size.Size
		if clog.V(3) {
			clog.Infof("And: Root: %p Total Cost: %v Best: %v", root, cost, bestCost)
		}
		if cost < bestCost {
			best = root
			bestCost = cost
		}
	}
	if clog.V(3) {
		clog.Infof("And: Choosing: %p Best: %v", best, bestCost)
	}

	// TODO(barakmich): Optimization of order need not stop here. Picking a smart
	// Contains() order based on probability of getting a false Contains() first is
	// useful (fail faster).

	var out []graph.IteratorShape
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

func sortByContainsCost(ctx context.Context, arr []graph.IteratorShape) error {
	cost := make([]graph.IteratorCosts, 0, len(arr))
	var last error
	for _, s := range arr {
		c, err := s.Stats(ctx)
		if err != nil {
			last = err
		}
		cost = append(cost, c)
	}
	sort.Sort(byCost{
		list: arr,
		cost: cost,
	})
	return last
}

// TODO(dennwc): store stats slice once
type byCost struct {
	list []graph.IteratorShape
	cost []graph.IteratorCosts
}

func (c byCost) Len() int { return len(c.list) }
func (c byCost) Less(i, j int) bool {
	return c.cost[i].ContainsCost < c.cost[j].ContainsCost
}
func (c byCost) Swap(i, j int) {
	c.list[i], c.list[j] = c.list[j], c.list[i]
	c.cost[i], c.cost[j] = c.cost[j], c.cost[i]
}

// optimizeContains() creates an alternate check list, containing the same contents
// but with a new ordering, however it wishes.
func (it *and) optimizeContains(ctx context.Context) error {
	// GetSubIterators allocates, so this is currently safe.
	// TODO(kortschak) Reuse it.checkList if possible.
	// This involves providing GetSubIterators with a slice to fill.
	// Generally this is a worthwhile thing to do in other places as well.
	it.checkList = append([]graph.IteratorShape{}, it.sub...)
	return sortByContainsCost(ctx, it.checkList)
}

// optimizeSubIterators(l) takes a list of iterators and calls Optimize() on all
// of them. It returns two lists -- the first contains the same list as l, where
// any replacements are made by Optimize() and the second contains the originals
// which were replaced.
func optimizeSubIterators(its []graph.Iterator) []graph.Iterator {
	out := make([]graph.Iterator, 0, len(its))
	for _, it := range its {
		o, _ := it.Optimize()
		out = append(out, o)
	}
	return out
}

// optimizeSubIterators(l) takes a list of iterators and calls Optimize() on all
// of them. It returns two lists -- the first contains the same list as l, where
// any replacements are made by Optimize() and the second contains the originals
// which were replaced.
func optimizeSubIterators2(ctx context.Context, its []graph.IteratorShape) []graph.IteratorShape {
	out := make([]graph.IteratorShape, 0, len(its))
	for _, it := range its {
		o, _ := it.Optimize(ctx)
		out = append(out, o)
	}
	return out
}

// Check a list of iterators for any Null iterators.
func hasAnyNullIterators(its []graph.IteratorShape) bool {
	for _, it := range its {
		if IsNull2(it) {
			return true
		}
	}
	return false
}

func materializeIts(ctx context.Context, its []graph.IteratorShape) ([]graph.IteratorShape, error) {
	var out []graph.IteratorShape

	allStats, stats, err := getStatsForSlice(ctx, its, nil)
	out = append(out, its[0])
	for i, it := range its[1:] {
		st := stats[i+1]
		if st.Size.Size*st.NextCost < (st.ContainsCost * (1 + (st.Size.Size / (allStats.Size.Size + 1)))) {
			if graph.Height(graph.AsLegacy(it), func(it graph.Iterator) bool {
				_, ok := it.(*Materialize)
				return !ok
			}) > 10 {
				out = append(out, newMaterialize(it))
				continue
			}
		}
		out = append(out, it)
	}
	return out, err
}

func getStatsForSlice(ctx context.Context, its, opt []graph.IteratorShape) (graph.IteratorCosts, []graph.IteratorCosts, error) {
	if len(its) == 0 {
		return graph.IteratorCosts{}, nil, nil
	}

	arr := make([]graph.IteratorCosts, 0, len(its))

	primaryStats, _ := its[0].Stats(ctx)
	arr = append(arr, primaryStats)

	containsCost := primaryStats.ContainsCost
	nextCost := primaryStats.NextCost
	size := primaryStats.Size.Size
	exact := primaryStats.Size.Exact

	var last error
	for _, sub := range its[1:] {
		stats, err := sub.Stats(ctx)
		if err != nil {
			last = err
		}
		arr = append(arr, stats)
		nextCost += stats.ContainsCost * (1 + (primaryStats.Size.Size / (stats.Size.Size + 1)))
		containsCost += stats.ContainsCost
		if size > stats.Size.Size {
			size = stats.Size.Size
			exact = stats.Size.Exact
		}
	}
	for _, sub := range opt {
		stats, _ := sub.Stats(ctx)
		nextCost += stats.ContainsCost * (1 + (primaryStats.Size.Size / (stats.Size.Size + 1)))
		containsCost += stats.ContainsCost
	}
	return graph.IteratorCosts{
		ContainsCost: containsCost,
		NextCost:     nextCost,
		Size: graph.Size{
			Size:  size,
			Exact: exact,
		},
	}, arr, last
}

// and.Stats() lives here in and-iterator-optimize.go because it may
// in the future return different statistics based on how it is optimized.
// For now, however, it's pretty static.
//
// Returns the approximate size of the And iterator. Because we're dealing
// with an intersection, we know that the largest we can be is the size of the
// smallest iterator. This is the heuristic we shall follow. Better heuristics
// welcome.
func (it *and) Stats(ctx context.Context) (graph.IteratorCosts, error) {
	stats, _, err := getStatsForSlice(ctx, it.sub, it.opt)
	return stats, err
}
