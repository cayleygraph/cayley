package graph

import "context"

type Iterator2Base interface {
	// String returns a short textual representation of an iterator.
	String() string

	// Fills a tag-to-result-value map.
	TagResults(map[string]Ref)

	// Returns the current result.
	Result() Ref

	// These methods are the heart and soul of the iterator, as they constitute
	// the iteration interface.
	//
	// To get the full results of iteration, do the following:
	//
	//  for graph.Next(it) {
	//  	val := it.Result()
	//  	... do things with val.
	//  	for it.NextPath() {
	//  		... find other paths to iterate
	//  	}
	//  }
	//
	// All of them should set iterator.result to be the last returned value, to
	// make results work.
	//
	// NextPath() advances iterators that may have more than one valid result,
	// from the bottom up.
	NextPath(ctx context.Context) bool

	// Err returns any error that was encountered by the Iterator.
	Err() error

	// TODO: make a requirement that Err should return ErrClosed after Close is called

	// Close the iterator and do internal cleanup.
	Close() error
}

type Iterator2Next interface {
	Iterator2Base

	// Next advances the iterator to the next value, which will then be available through
	// the Result method. It returns false if no further advancement is possible, or if an
	// error was encountered during iteration.  Err should be consulted to distinguish
	// between the two cases.
	Next(ctx context.Context) bool
}

type Iterator2Contains interface {
	Iterator2Base

	// Contains returns whether the value is within the set held by the iterator.
	//
	// It will set Result to the matching subtree. TagResults can be used to collect values from tree branches.
	Contains(ctx context.Context, v Ref) bool
}

type Iterator2 interface {
	// TODO(dennwc) this is a Shape, in fact

	// String returns a short textual representation of an iterator.
	String() string

	Iterate() Iterator2Next
	Lookup() Iterator2Contains

	// These methods relate to choosing the right iterator, or optimizing an
	// iterator tree
	//
	// Stats() returns the relative costs of calling the iteration methods for
	// this iterator, as well as the size. Roughly, it will take NextCost * Size
	// "cost units" to get everything out of the iterator. This is a wibbly-wobbly
	// thing, and not exact, but a useful heuristic.
	Stats() IteratorStats

	// Helpful accessor for the number of things in the iterator. The first return
	// value is the size, and the second return value is whether that number is exact,
	// or a conservative estimate.
	Size() (int64, bool)

	// Optimizes an iterator. Can replace the iterator, or merely move things
	// around internally. if it chooses to replace it with a better iterator,
	// returns (the new iterator, true), if not, it returns (self, false).
	Optimize() (Iterator2, bool)

	// Return a slice of the subiterators for this iterator.
	SubIterators() []Iterator2
}

type Iterator2Compat interface {
	Iterator2
	AsLegacy() Iterator
}

func As2(it Iterator) Iterator2 {
	if it == nil {
		panic("nil iterator")
	}
	if it2, ok := it.(*legacyIter); ok {
		it2.Close()
		return it2.it
	}
	return &upgrade{it}
}

func AsNext(it Iterator) Iterator2Next {
	if it == nil {
		panic("nil iterator")
	}
	if it2, ok := it.(*legacyIter); ok {
		it2.Close()
		return it2.it.Iterate()
	}
	return &upgrade{it}
}

func AsContains(it Iterator) Iterator2Contains {
	if it == nil {
		panic("nil iterator")
	}
	if it2, ok := it.(*legacyIter); ok {
		it2.Close()
		return it2.it.Lookup()
	}
	return &upgrade{it}
}

var _ Iterator2Compat = &upgrade{}

type upgrade struct {
	Iterator
}

func (it *upgrade) Optimize() (Iterator2, bool) {
	nit, ok := it.Iterator.Optimize()
	if !ok {
		return it, false
	}
	return As2(nit), true
}

func (it *upgrade) SubIterators() []Iterator2 {
	its := it.Iterator.SubIterators()
	out := make([]Iterator2, 0, len(its))
	for _, s := range its {
		out = append(out, As2(s))
	}
	return out
}

func (it *upgrade) Iterate() Iterator2Next {
	it.Reset()
	return it
}
func (it *upgrade) Lookup() Iterator2Contains {
	it.Reset()
	return it
}
func (it *upgrade) Close() error {
	// FIXME(dennwc): this is incorrect, but we must do this to prevent closing iterators after
	//                multiple calls to Iterate and/or Lookup
	return nil
}
func (it *upgrade) AsLegacy() Iterator {
	return it.Iterator
}

func AsLegacy(it Iterator2) Iterator {
	if it == nil {
		panic("nil iterator")
	}
	if it2, ok := it.(Iterator2Compat); ok {
		return it2.AsLegacy()
	}
	return &legacyIter{it: it}
}

type legacyIter struct {
	it   Iterator2
	next Iterator2Next
	cont Iterator2Contains
}

func (it *legacyIter) String() string {
	return it.it.String()
}

func (it *legacyIter) TagResults(m map[string]Ref) {
	if it.cont != nil && it.next != nil {
		panic("both iterators are set")
	}
	if it.next != nil {
		it.next.TagResults(m)
	} else if it.cont != nil {
		it.cont.TagResults(m)
	}
}

func (it *legacyIter) Result() Ref {
	if it.cont != nil && it.next != nil {
		panic("both iterators are set")
	}
	if it.next != nil {
		return it.next.Result()
	}
	if it.cont != nil {
		return it.cont.Result()
	}
	return nil
}

func (it *legacyIter) Next(ctx context.Context) bool {
	if it.next == nil {
		if it.cont != nil {
			panic("attempt to set a next iterator on contains")
		}
		it.next = it.it.Iterate()
	}
	return it.next.Next(ctx)
}

func (it *legacyIter) NextPath(ctx context.Context) bool {
	if it.cont != nil && it.next != nil {
		panic("both iterators are set")
	}
	if it.next != nil {
		return it.next.NextPath(ctx)
	}
	if it.cont != nil {
		return it.cont.NextPath(ctx)
	}
	panic("calling NextPath before Next or Contains")
}

func (it *legacyIter) Contains(ctx context.Context, v Ref) bool {
	if it.cont == nil {
		// reset iterator by default
		if it.next != nil {
			it.next.Close()
			it.next = nil
		}
		it.cont = it.it.Lookup()
	}
	return it.cont.Contains(ctx, v)
}

func (it *legacyIter) Err() error {
	if it.next != nil {
		if err := it.next.Err(); err != nil {
			return err
		}
	}
	if it.cont != nil {
		if err := it.cont.Err(); err != nil {
			return err
		}
	}
	return nil
}

func (it *legacyIter) Reset() {
	if it.next != nil {
		it.next.Close()
		it.next = it.it.Iterate()
	}
	if it.cont != nil {
		it.cont.Close()
		it.cont = it.it.Lookup()
	}
}

func (it *legacyIter) Stats() IteratorStats {
	return it.it.Stats()
}

func (it *legacyIter) Size() (int64, bool) {
	return it.it.Size()
}

func (it *legacyIter) Optimize() (Iterator, bool) {
	nit, ok := it.it.Optimize()
	if !ok {
		return it, false
	}
	return AsLegacy(nit), true
}

func (it *legacyIter) SubIterators() []Iterator {
	its := it.it.SubIterators()
	out := make([]Iterator, 0, len(its))
	for _, s := range its {
		out = append(out, AsLegacy(s))
	}
	return out
}

func (it *legacyIter) Close() error {
	if it.next != nil {
		it.next.Close()
		it.next = nil
	}
	if it.cont != nil {
		it.cont.Close()
		it.cont = nil
	}
	return nil
}
