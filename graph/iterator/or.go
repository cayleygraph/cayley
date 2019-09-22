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

// Defines the or and short-circuiting or iterator. Or is the union operator for it's subiterators.
// Short-circuiting-or is a little different. It will return values from the first graph.iterator that returns
// values at all, and then stops.
//
// Never reorders the iterators from the order they arrive. It is either the union or the first one.
// May return the same value twice -- once for each branch.

import (
	"context"

	"github.com/cayleygraph/cayley/graph"
)

var _ graph.IteratorFuture = &Or{}

type Or struct {
	it *or
	graph.Iterator
}

func NewOr(sub ...graph.Iterator) *Or {
	in := make([]graph.IteratorShape, 0, len(sub))
	for _, s := range sub {
		in = append(in, graph.AsShape(s))
	}
	it := &Or{
		it: newOr(in...),
	}
	it.Iterator = graph.NewLegacy(it.it, it)
	return it
}

func NewShortCircuitOr(sub ...graph.Iterator) *Or {
	in := make([]graph.IteratorShape, 0, len(sub))
	for _, s := range sub {
		in = append(in, graph.AsShape(s))
	}
	it := &Or{
		it: newShortCircuitOr(in...),
	}
	it.Iterator = graph.NewLegacy(it.it, it)
	return it
}

func (it *Or) AsShape() graph.IteratorShape {
	it.Close()
	return it.it
}

// Add a subiterator to this Or graph.iterator. Order matters.
func (it *Or) AddSubIterator(sub graph.Iterator) {
	it.it.AddSubIterator(graph.AsShape(sub))
}

var _ graph.IteratorShapeCompat = &or{}

type or struct {
	isShortCircuiting bool
	sub               []graph.IteratorShape
	curInd            int
	result            graph.Ref
	err               error
}

func newOr(sub ...graph.IteratorShape) *or {
	it := &or{
		sub:    make([]graph.IteratorShape, 0, 20),
		curInd: -1,
	}
	for _, s := range sub {
		it.AddSubIterator(s)
	}
	return it
}

func newShortCircuitOr(sub ...graph.IteratorShape) *or {
	it := &or{
		sub:               make([]graph.IteratorShape, 0, 20),
		isShortCircuiting: true,
		curInd:            -1,
	}
	for _, s := range sub {
		it.AddSubIterator(s)
	}
	return it
}

func (it *or) Iterate() graph.Scanner {
	sub := make([]graph.Scanner, 0, len(it.sub))
	for _, s := range it.sub {
		sub = append(sub, s.Iterate())
	}
	return newOrNext(sub, it.isShortCircuiting)
}

func (it *or) Lookup() graph.Index {
	sub := make([]graph.Index, 0, len(it.sub))
	for _, s := range it.sub {
		sub = append(sub, s.Lookup())
	}
	return newOrContains(sub, it.isShortCircuiting)
}

func (it *or) AsLegacy() graph.Iterator {
	it2 := &Or{it: it}
	it2.Iterator = graph.NewLegacy(it, it2)
	return it2
}

// Returns a list.List of the subiterators, in order. The returned slice must not be modified.
func (it *or) SubIterators() []graph.IteratorShape {
	return it.sub
}

func (it *or) String() string {
	return "Or"
}

// Add a subiterator to this Or graph.iterator. Order matters.
func (it *or) AddSubIterator(sub graph.IteratorShape) {
	it.sub = append(it.sub, sub)
}

func (it *or) Optimize(ctx context.Context) (graph.IteratorShape, bool) {
	old := it.SubIterators()
	optIts := optimizeSubIterators2(ctx, old)
	newOr := newOr()
	newOr.isShortCircuiting = it.isShortCircuiting

	// Add the subiterators in order.
	for _, o := range optIts {
		newOr.AddSubIterator(o)
	}
	return newOr, true
}

// Returns the approximate size of the Or graph.iterator. Because we're dealing
// with a union, we know that the largest we can be is the sum of all the iterators,
// or in the case of short-circuiting, the longest.
func (it *or) Stats(ctx context.Context) (graph.IteratorCosts, error) {
	ContainsCost := int64(0)
	NextCost := int64(0)
	Size := graph.Size{
		Size:  0,
		Exact: true,
	}
	var last error
	for _, sub := range it.sub {
		stats, err := sub.Stats(ctx)
		if err != nil {
			last = err
		}
		NextCost += stats.NextCost
		ContainsCost += stats.ContainsCost
		if it.isShortCircuiting {
			if Size.Size < stats.Size.Size {
				Size = stats.Size
			}
		} else {
			Size.Size += stats.Size.Size
			Size.Exact = Size.Exact && stats.Size.Exact
		}
	}
	return graph.IteratorCosts{
		ContainsCost: ContainsCost,
		NextCost:     NextCost,
		Size:         Size,
	}, last
}

type orNext struct {
	shortCircuit bool
	sub          []graph.Scanner
	curInd       int
	result       graph.Ref
	err          error
}

func newOrNext(sub []graph.Scanner, shortCircuit bool) *orNext {
	return &orNext{
		sub:          sub,
		curInd:       -1,
		shortCircuit: shortCircuit,
	}
}

// Overrides BaseIterator TagResults, as it needs to add it's own results and
// recurse down it's subiterators.
func (it *orNext) TagResults(dst map[string]graph.Ref) {
	it.sub[it.curInd].TagResults(dst)
}

func (it *orNext) String() string {
	return "OrNext"
}

// Next advances the Or graph.iterator. Because the Or is the union of its
// subiterators, it must produce from all subiterators -- unless it it
// shortcircuiting, in which case, it is the first one that returns anything.
func (it *orNext) Next(ctx context.Context) bool {
	if it.curInd >= len(it.sub) {
		return false
	}
	var first bool
	for {
		if it.curInd == -1 {
			it.curInd = 0
			first = true
		}
		curIt := it.sub[it.curInd]

		if curIt.Next(ctx) {
			it.result = curIt.Result()
			return true
		}

		it.err = curIt.Err()
		if it.err != nil {
			return false
		}

		if it.shortCircuit && !first {
			break
		}
		it.curInd++
		if it.curInd >= len(it.sub) {
			break
		}
	}

	return false
}

func (it *orNext) Err() error {
	return it.err
}

func (it *orNext) Result() graph.Ref {
	return it.result
}

// An Or has no NextPath of its own -- that is, there are no other values
// which satisfy our previous result that are not the result itself. Our
// subiterators might, however, so just pass the call recursively. In the case of
// shortcircuiting, only allow new results from the currently checked graph.iterator
func (it *orNext) NextPath(ctx context.Context) bool {
	if it.curInd != -1 {
		currIt := it.sub[it.curInd]
		ok := currIt.NextPath(ctx)
		if !ok {
			it.err = currIt.Err()
		}
		return ok
	}
	return false
}

// Close this graph.iterator, and, by extension, close the subiterators.
// Close should be idempotent, and it follows that if it's subiterators
// follow this contract, the Or follows the contract.  It closes all
// subiterators it can, but returns the first error it encounters.
func (it *orNext) Close() error {
	var err error
	for _, sub := range it.sub {
		_err := sub.Close()
		if _err != nil && err == nil {
			err = _err
		}
	}
	return err
}

var _ graph.Iterator = &Or{}

type orContains struct {
	shortCircuit bool
	sub          []graph.Index
	curInd       int
	result       graph.Ref
	err          error
}

func newOrContains(sub []graph.Index, shortCircuit bool) *orContains {
	return &orContains{
		sub:          sub,
		curInd:       -1,
		shortCircuit: shortCircuit,
	}
}

// Overrides BaseIterator TagResults, as it needs to add it's own results and
// recurse down it's subiterators.
func (it *orContains) TagResults(dst map[string]graph.Ref) {
	it.sub[it.curInd].TagResults(dst)
}

func (it *orContains) String() string {
	return "OrContains"
}

func (it *orContains) Err() error {
	return it.err
}

func (it *orContains) Result() graph.Ref {
	return it.result
}

// Checks a value against the iterators, in order.
func (it *orContains) subItsContain(ctx context.Context, val graph.Ref) (bool, error) {
	subIsGood := false
	for i, sub := range it.sub {
		subIsGood = sub.Contains(ctx, val)
		if subIsGood {
			it.curInd = i
			break
		}

		err := sub.Err()
		if err != nil {
			return false, err
		}
	}
	return subIsGood, nil
}

// Check a value against the entire graph.iterator, in order.
func (it *orContains) Contains(ctx context.Context, val graph.Ref) bool {
	anyGood, err := it.subItsContain(ctx, val)
	if err != nil {
		it.err = err
		return false
	} else if !anyGood {
		return false
	}
	it.result = val
	return true
}

// An Or has no NextPath of its own -- that is, there are no other values
// which satisfy our previous result that are not the result itself. Our
// subiterators might, however, so just pass the call recursively. In the case of
// shortcircuiting, only allow new results from the currently checked graph.iterator
func (it *orContains) NextPath(ctx context.Context) bool {
	if it.curInd != -1 {
		currIt := it.sub[it.curInd]
		ok := currIt.NextPath(ctx)
		if !ok {
			it.err = currIt.Err()
		}
		return ok
	}
	// TODO(dennwc): this should probably list matches from other sub-iterators
	return false
}

// Close this graph.iterator, and, by extension, close the subiterators.
// Close should be idempotent, and it follows that if it's subiterators
// follow this contract, the Or follows the contract.  It closes all
// subiterators it can, but returns the first error it encounters.
func (it *orContains) Close() error {
	var err error
	for _, sub := range it.sub {
		_err := sub.Close()
		if _err != nil && err == nil {
			err = _err
		}
	}
	return err
}
