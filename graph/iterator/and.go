// Defines the And iterator, one of the base iterators. And requires no
// knowledge of the constituent QuadStore; its sole purpose is to act as an
// intersection operator across the subiterators it is given. If one iterator
// contains [1,3,5] and another [2,3,4] -- then And is an iterator that
// 'contains' [3]
//
// It accomplishes this in one of two ways. If it is a Next()ed iterator (that
// is, it is a top level iterator, or on the "Next() path", then it will Next()
// it's primary iterator (helpfully, and.primary_it) and Contains() the resultant
// value against it's other iterators. If it matches all of them, then it
// returns that value. Otherwise, it repeats the process.
//
// If it's on a Contains() path, it merely Contains()s every iterator, and returns the
// logical AND of each result.

package iterator

import (
	"context"

	"github.com/cayleygraph/cayley/graph"
)

var _ graph.IteratorFuture = &And{}

// The And iterator. Consists of a number of subiterators, the primary of which will
// be Next()ed if next is called.
type And struct {
	it *and2
	graph.Iterator
}

// NewAnd creates an And iterator. `qs` is only required when needing a handle
// for QuadStore-specific optimizations, otherwise nil is acceptable.
func NewAnd(sub ...graph.Iterator) *And {
	it := &And{
		it: newAnd(),
	}
	for _, s := range sub {
		it.it.AddSubIterator(graph.As2(s))
	}
	it.Iterator = graph.AsLegacy(it.it)
	return it
}

func (it *And) As2() graph.Iterator2 {
	it.Close()
	return it.it
}

// Add a subiterator to this And iterator.
//
// The first iterator that is added becomes the primary iterator. This is
// important. Calling Optimize() is the way to change the order based on
// subiterator statistics. Without Optimize(), the order added is the order
// used.
func (it *And) AddSubIterator(sub graph.Iterator) {
	it.it.AddSubIterator(graph.As2(sub))
}

// AddOptionalIterator adds an iterator that will only be Contain'ed and will not affect iteration results.
// Only tags will be propagated from this iterator.
func (it *And) AddOptionalIterator(sub graph.Iterator) *And {
	it.it.AddOptionalIterator(graph.As2(sub))
	return it
}

var _ graph.Iterator2Compat = &and2{}

// The And iterator. Consists of a number of subiterators, the primary of which will
// be Next()ed if next is called.
type and2 struct {
	sub       []graph.Iterator2
	checkList []graph.Iterator2 // special order for Contains
	opt       []graph.Iterator2
}

// NewAnd creates an And iterator. `qs` is only required when needing a handle
// for QuadStore-specific optimizations, otherwise nil is acceptable.
func newAnd(sub ...graph.Iterator2) *and2 {
	it := &and2{
		sub: make([]graph.Iterator2, 0, 20),
	}
	for _, s := range sub {
		it.AddSubIterator(s)
	}
	return it
}

func (it *and2) Iterate() graph.Iterator2Next {
	if len(it.sub) == 0 {
		return newNull2().Iterate()
	}
	sub := make([]graph.Iterator2Contains, 0, len(it.sub)-1)
	for _, s := range it.sub[1:] {
		sub = append(sub, s.Lookup())
	}
	opt := make([]graph.Iterator2Contains, 0, len(it.opt))
	for _, s := range it.opt {
		opt = append(opt, s.Lookup())
	}
	return newAndNext(it.sub[0].Iterate(), newAndContains(sub, opt))
}

func (it *and2) Lookup() graph.Iterator2Contains {
	if len(it.sub) == 0 {
		return newNull2().Lookup()
	}
	sub := make([]graph.Iterator2Contains, 0, len(it.sub))
	check := it.checkList
	if check == nil {
		check = it.sub
	}
	for _, s := range check {
		sub = append(sub, s.Lookup())
	}
	opt := make([]graph.Iterator2Contains, 0, len(it.opt))
	for _, s := range it.opt {
		opt = append(opt, s.Lookup())
	}
	return newAndContains(sub, opt)
}

func (it *and2) AsLegacy() graph.Iterator {
	it2 := &And{it: it}
	it2.Iterator = graph.NewLegacy(it)
	return it2
}

// Returns a slice of the subiterators, in order (primary iterator first).
func (it *and2) SubIterators() []graph.Iterator2 {
	iters := make([]graph.Iterator2, 0, len(it.sub)+len(it.opt))
	iters = append(iters, it.sub...)
	iters = append(iters, it.opt...)
	return iters
}

func (it *and2) String() string {
	return "And"
}

// Add a subiterator to this And iterator.
//
// The first iterator that is added becomes the primary iterator. This is
// important. Calling Optimize() is the way to change the order based on
// subiterator statistics. Without Optimize(), the order added is the order
// used.
func (it *and2) AddSubIterator(sub graph.Iterator2) {
	if sub == nil {
		panic("nil iterator")
	}
	it.sub = append(it.sub, sub)
}

// AddOptionalIterator adds an iterator that will only be Contain'ed and will not affect iteration results.
// Only tags will be propagated from this iterator.
func (it *and2) AddOptionalIterator(sub graph.Iterator2) *and2 {
	it.opt = append(it.opt, sub)
	return it
}

// Returns the approximate size of the And iterator. Because we're dealing
// with an intersection, we know that the largest we can be is the size of the
// smallest iterator. This is the heuristic we shall follow. Better heuristics
// welcome.
func (it *and2) Size() (sz int64, exact bool) {
	exact = true
	for i, sub := range it.sub {
		sz2, exact2 := sub.Size()
		if i == 0 || sz > sz2 {
			sz = sz2
		}
		exact = exact2 && exact
	}
	return sz, exact
}

// The And iterator. Consists of a number of subiterators, the primary of which will
// be Next()ed if next is called.
type andNext struct {
	primary   graph.Iterator2Next
	secondary graph.Iterator2Contains
	result    graph.Ref
}

// NewAnd creates an And iterator. `qs` is only required when needing a handle
// for QuadStore-specific optimizations, otherwise nil is acceptable.
func newAndNext(pri graph.Iterator2Next, sec graph.Iterator2Contains) graph.Iterator2Next {
	return &andNext{
		primary:   pri,
		secondary: sec,
	}
}

// An extended TagResults, as it needs to add it's own results and
// recurse down it's subiterators.
func (it *andNext) TagResults(dst map[string]graph.Ref) {
	it.primary.TagResults(dst)
	it.secondary.TagResults(dst)
}

func (it *andNext) String() string {
	return "AndNext"
}

// Returns advances the And iterator. Because the And is the intersection of its
// subiterators, it must choose one subiterator to produce a candidate, and check
// this value against the subiterators. A productive choice of primary iterator
// is therefore very important.
func (it *andNext) Next(ctx context.Context) bool {
	for it.primary.Next(ctx) {
		cur := it.primary.Result()
		if it.secondary.Contains(ctx, cur) {
			it.result = cur
			return true
		}
	}
	return false
}

func (it *andNext) Err() error {
	if err := it.primary.Err(); err != nil {
		return err
	}
	if err := it.secondary.Err(); err != nil {
		return err
	}
	return nil
}

func (it *andNext) Result() graph.Ref {
	return it.result
}

// An And has no NextPath of its own -- that is, there are no other values
// which satisfy our previous result that are not the result itself. Our
// subiterators might, however, so just pass the call recursively.
func (it *andNext) NextPath(ctx context.Context) bool {
	if it.primary.NextPath(ctx) {
		return true
	} else if err := it.primary.Err(); err != nil {
		return false
	}
	if it.secondary.NextPath(ctx) {
		return true
	} else if err := it.secondary.Err(); err != nil {
		return false
	}
	return false
}

// Close this iterator, and, by extension, close the subiterators.
// Close should be idempotent, and it follows that if it's subiterators
// follow this contract, the And follows the contract.  It closes all
// subiterators it can, but returns the first error it encounters.
func (it *andNext) Close() error {
	err := it.primary.Close()
	if err2 := it.secondary.Close(); err2 != nil && err == nil {
		err = err2
	}
	return err
}

// The And iterator. Consists of a number of subiterators, the primary of which will
// be Next()ed if next is called.
type andContains struct {
	base     graph.Iterator2
	sub      []graph.Iterator2Contains
	opt      []graph.Iterator2Contains
	optCheck []bool

	result graph.Ref
	err    error
}

// NewAnd creates an And iterator. `qs` is only required when needing a handle
// for QuadStore-specific optimizations, otherwise nil is acceptable.
func newAndContains(sub, opt []graph.Iterator2Contains) graph.Iterator2Contains {
	return &andContains{
		sub: sub,
		opt: opt, optCheck: make([]bool, len(opt)),
	}
}

// An extended TagResults, as it needs to add it's own results and
// recurse down it's subiterators.
func (it *andContains) TagResults(dst map[string]graph.Ref) {
	for _, sub := range it.sub {
		sub.TagResults(dst)
	}
	for i, sub := range it.opt {
		if !it.optCheck[i] {
			continue
		}
		sub.TagResults(dst)
	}
}

func (it *andContains) String() string {
	return "AndContains"
}

func (it *andContains) Err() error {
	if err := it.err; err != nil {
		return err
	}
	for _, si := range it.sub {
		if err := si.Err(); err != nil {
			return err
		}
	}
	for _, si := range it.opt {
		if err := si.Err(); err != nil {
			return err
		}
	}
	return nil
}

func (it *andContains) Result() graph.Ref {
	return it.result
}

// Check a value against the entire iterator, in order.
func (it *andContains) Contains(ctx context.Context, val graph.Ref) bool {
	prev := it.result
	for i, sub := range it.sub {
		if !sub.Contains(ctx, val) {
			if err := sub.Err(); err != nil {
				it.err = err
				return false
			}
			// One of the iterators has determined that this value doesn't
			// match. However, the iterators that came before in the list
			// may have returned "ok" to Contains().  We need to set all
			// the tags back to what the previous result was -- effectively
			// seeking back exactly one -- so we check all the prior iterators
			// with the (already verified) result and throw away the result,
			// which will be 'true'
			if prev != nil {
				for j := 0; j < i; j++ {
					it.sub[j].Contains(ctx, prev)
					if err := it.sub[j].Err(); err != nil {
						it.err = err
						return false
					}
				}
			}
			return false
		}
	}
	it.result = val
	for i, sub := range it.opt {
		// remember if we will need to call TagResults on it, nothing more
		it.optCheck[i] = sub.Contains(ctx, val)
	}
	return true
}

// An And has no NextPath of its own -- that is, there are no other values
// which satisfy our previous result that are not the result itself. Our
// subiterators might, however, so just pass the call recursively.
func (it *andContains) NextPath(ctx context.Context) bool {
	for _, sub := range it.sub {
		if sub.NextPath(ctx) {
			return true
		} else if err := sub.Err(); err != nil {
			it.err = err
			return false
		}
	}
	for i, sub := range it.opt {
		if !it.optCheck[i] {
			continue
		}
		if sub.NextPath(ctx) {
			return true
		} else if err := sub.Err(); err != nil {
			it.err = err
			return false
		}
	}
	return false
}

// Close this iterator, and, by extension, close the subiterators.
// Close should be idempotent, and it follows that if it's subiterators
// follow this contract, the And follows the contract.  It closes all
// subiterators it can, but returns the first error it encounters.
func (it *andContains) Close() error {
	var err error
	for _, sub := range it.sub {
		if err2 := sub.Close(); err2 != nil && err == nil {
			err = err2
		}
	}
	for _, sub := range it.opt {
		if err2 := sub.Close(); err2 != nil && err == nil {
			err = err2
		}
	}
	return err
}
