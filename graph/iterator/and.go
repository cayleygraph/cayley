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

var _ graph.Iterator = &And{}

// The And iterator. Consists of a number of subiterators, the primary of which will
// be Next()ed if next is called.
type And struct {
	uid uint64

	primary   graph.Iterator
	sub       []graph.Iterator
	opt       []graph.Iterator
	optCheck  []bool
	checkList []graph.Iterator

	runstats graph.IteratorStats
	result   graph.Value
	err      error
}

// NewAnd creates an And iterator. `qs` is only required when needing a handle
// for QuadStore-specific optimizations, otherwise nil is acceptable.
func NewAnd(sub ...graph.Iterator) *And {
	it := &And{
		uid: NextUID(),
		sub: make([]graph.Iterator, 0, 20),
	}
	for _, s := range sub {
		it.AddSubIterator(s)
	}
	return it
}

func (it *And) UID() uint64 {
	return it.uid
}

// Reset all internal iterators
func (it *And) Reset() {
	it.result = nil
	it.primary.Reset()
	for _, sub := range it.sub {
		sub.Reset()
	}
	it.checkList = nil
}

// An extended TagResults, as it needs to add it's own results and
// recurse down it's subiterators.
func (it *And) TagResults(dst map[string]graph.Value) {
	if it.primary != nil {
		it.primary.TagResults(dst)
	}
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

func (it *And) Clone() graph.Iterator {
	and := NewAnd()
	and.AddSubIterator(it.primary.Clone())
	for _, sub := range it.sub {
		and.AddSubIterator(sub.Clone())
	}
	for _, sub := range it.opt {
		and.AddOptionalIterator(sub.Clone())
	}
	if it.checkList != nil {
		and.optimizeContains()
	}
	return and
}

// subIterators is like SubIterators but excludes optional.
func (it *And) subIterators() []graph.Iterator {
	iters := make([]graph.Iterator, 0, 1+len(it.sub))
	if it.primary != nil {
		iters = append(iters, it.primary)
	}
	iters = append(iters, it.sub...)
	// exclude optional
	return iters
}

// Returns a slice of the subiterators, in order (primary iterator first).
func (it *And) SubIterators() []graph.Iterator {
	iters := make([]graph.Iterator, 0, 1+len(it.sub)+len(it.opt))
	if it.primary != nil {
		iters = append(iters, it.primary)
	}
	iters = append(iters, it.sub...)
	iters = append(iters, it.opt...)
	return iters
}

func (it *And) String() string {
	return "And"
}

// Add a subiterator to this And iterator.
//
// The first iterator that is added becomes the primary iterator. This is
// important. Calling Optimize() is the way to change the order based on
// subiterator statistics. Without Optimize(), the order added is the order
// used.
func (it *And) AddSubIterator(sub graph.Iterator) {
	if it.primary == nil {
		it.primary = sub
		return
	}
	it.sub = append(it.sub, sub)
}

// AddOptionalIterator adds an iterator that will only be Contain'ed and will not affect iteration results.
// Only tags will be propagated from this iterator.
func (it *And) AddOptionalIterator(sub graph.Iterator) *And {
	it.opt = append(it.opt, sub)
	it.optCheck = append(it.optCheck, false)
	return it
}

// Returns advances the And iterator. Because the And is the intersection of its
// subiterators, it must choose one subiterator to produce a candidate, and check
// this value against the subiterators. A productive choice of primary iterator
// is therefore very important.
func (it *And) Next(ctx context.Context) bool {
	it.runstats.Next += 1
	for it.primary.Next(ctx) {
		cur := it.primary.Result()
		if it.subContain(ctx, cur, nil) {
			it.result = cur
			return true
		}
	}
	it.err = it.primary.Err()
	return false
}

func (it *And) Err() error {
	if err := it.err; err != nil {
		return err
	}
	if err := it.primary.Err(); err != nil {
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

func (it *And) Result() graph.Value {
	return it.result
}

func (it *And) checkOpt(ctx context.Context, val graph.Value) {
	for i, sub := range it.opt {
		// remember if we will need to call TagResults on it, nothing more
		it.optCheck[i] = sub.Contains(ctx, val)
	}
}

func (it *And) allContains(ctx context.Context, check []graph.Iterator, val graph.Value, prev graph.Value) bool {
	for i, sub := range check {
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
					check[j].Contains(ctx, prev)
					if err := check[j].Err(); err != nil {
						it.err = err
						return false
					}
				}
			}
			return false
		}
	}
	it.result = val
	it.checkOpt(ctx, val)
	return true
}

// subContain checks a value against the non-primary iterators, in order.
func (it *And) subContain(ctx context.Context, cur graph.Value, prev graph.Value) bool {
	return it.allContains(ctx, it.sub, cur, prev)
}

// checkContain is like subContain but uses optimized order of iterators stored in it.checkList, which includes primary.
func (it *And) checkContain(ctx context.Context, cur graph.Value, prev graph.Value) bool {
	return it.allContains(ctx, it.checkList, cur, prev)
}

// Check a value against the entire iterator, in order.
func (it *And) Contains(ctx context.Context, val graph.Value) bool {
	it.runstats.Contains += 1
	prev := it.result
	if it.checkList != nil {
		return it.checkContain(ctx, val, prev)
	}
	if it.primary.Contains(ctx, val) && it.subContain(ctx, val, prev) {
		it.result = val
		return true
	}
	if prev != nil {
		it.primary.Contains(ctx, prev)
	}
	return false
}

// Returns the approximate size of the And iterator. Because we're dealing
// with an intersection, we know that the largest we can be is the size of the
// smallest iterator. This is the heuristic we shall follow. Better heuristics
// welcome.
func (it *And) Size() (int64, bool) {
	sz, exact := it.primary.Size()
	for _, sub := range it.sub {
		sz2, exact2 := sub.Size()
		if sz > sz2 {
			sz = sz2
		}
		exact = exact2 && exact
	}
	return sz, exact
}

// An And has no NextPath of its own -- that is, there are no other values
// which satisfy our previous result that are not the result itself. Our
// subiterators might, however, so just pass the call recursively.
func (it *And) NextPath(ctx context.Context) bool {
	if it.primary.NextPath(ctx) {
		return true
	} else if err := it.primary.Err(); err != nil {
		it.err = err
		return false
	}
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
func (it *And) Close() error {
	err := it.primary.Close()
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

// Register this as an "and" iterator.
func (it *And) Type() graph.Type { return graph.And }
