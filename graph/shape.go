package graph

import "context"

// Scanner is an iterator that lists all results sequentially, but not necessarily in a sorted order.
type Scanner interface {
	IteratorBase

	// Next advances the iterator to the next value, which will then be available through
	// the Result method. It returns false if no further advancement is possible, or if an
	// error was encountered during iteration.  Err should be consulted to distinguish
	// between the two cases.
	Next(ctx context.Context) bool
}

// Index is an index lookup iterator. It allows to check if an index contains a specific value.
type Index interface {
	IteratorBase

	// Contains returns whether the value is within the set held by the iterator.
	//
	// It will set Result to the matching subtree. TagResults can be used to collect values from tree branches.
	Contains(ctx context.Context, v Ref) bool
}

// Tagger is an interface for iterators that can tag values. Tags are returned as a part of TagResults call.
type TaggerShape interface {
	IteratorShape
	TaggerBase
	CopyFromTagger(st TaggerBase)
}

type IteratorCosts struct {
	ContainsCost int64
	NextCost     int64
	Size         Size
}

// Shape is an iterator shape, similar to a query plan. But the plan is not specific in this
// case - it is used to reorder query branches, and the decide what branches will be scanned
// and what branches will lookup values (hopefully from the index, but not necessarily).
type IteratorShape interface {
	// TODO(dennwc): merge with shape.Shape

	// String returns a short textual representation of an iterator.
	String() string

	// Iterate starts this iterator in scanning mode. Resulting iterator will list all
	// results sequentially, but not necessary in the sorted order. Caller must close
	// the iterator.
	Iterate() Scanner

	// Lookup starts this iterator in an index lookup mode. Depending on the iterator type,
	// this may still involve database scans. Resulting iterator allows to check an index
	// contains a specified value. Caller must close the iterator.
	Lookup() Index

	// These methods relate to choosing the right iterator, or optimizing an
	// iterator tree
	//
	// Stats() returns the relative costs of calling the iteration methods for
	// this iterator, as well as the size. Roughly, it will take NextCost * Size
	// "cost units" to get everything out of the iterator. This is a wibbly-wobbly
	// thing, and not exact, but a useful heuristic.
	Stats(ctx context.Context) (IteratorCosts, error)

	// Optimizes an iterator. Can replace the iterator, or merely move things
	// around internally. if it chooses to replace it with a better iterator,
	// returns (the new iterator, true), if not, it returns (self, false).
	Optimize(ctx context.Context) (IteratorShape, bool)

	// Return a slice of the subiterators for this iterator.
	SubIterators() []IteratorShape
}

// IteratorShapeCompat is an optional interface for iterator Shape that support direct conversion
// to a legacy Iterator. This interface should be avoided an will be deprecated in the future.
type IteratorShapeCompat interface {
	IteratorShape
	AsLegacy() Iterator
}

// AsShape converts a legacy Iterator to an iterator Shape.
func AsShape(it Iterator) IteratorShape {
	if it == nil {
		panic("nil iterator")
	}
	if it2, ok := it.(IteratorFuture); ok {
		return it2.AsShape()
	}
	return &legacyShape{it}
}

var _ IteratorShapeCompat = &legacyShape{}

type legacyShape struct {
	Iterator
}

func (it *legacyShape) Optimize(ctx context.Context) (IteratorShape, bool) {
	nit, ok := it.Iterator.Optimize()
	if !ok {
		return it, false
	}
	return AsShape(nit), true
}

func (it *legacyShape) SubIterators() []IteratorShape {
	its := it.Iterator.SubIterators()
	out := make([]IteratorShape, 0, len(its))
	for _, s := range its {
		out = append(out, AsShape(s))
	}
	return out
}

func (it *legacyShape) Stats(ctx context.Context) (IteratorCosts, error) {
	st := it.Iterator.Stats()
	return IteratorCosts{
		NextCost:     st.NextCost,
		ContainsCost: st.ContainsCost,
		Size: Size{
			Size:  st.Size,
			Exact: st.ExactSize,
		},
	}, it.Err()
}

func (it *legacyShape) Iterate() Scanner {
	it.Reset()
	return it
}
func (it *legacyShape) Lookup() Index {
	it.Reset()
	return it
}
func (it *legacyShape) Close() error {
	// FIXME(dennwc): this is incorrect, but we must do this to prevent closing iterators after
	//                multiple calls to Iterate and/or Lookup
	return nil
}
func (it *legacyShape) AsLegacy() Iterator {
	return it.Iterator
}

// NewLegacy creates a new legacy Iterator from an iterator Shape.
// This method will always create a new iterator, while AsLegacy will try to unwrap it first.
func NewLegacy(s IteratorShape, self Iterator) Iterator {
	if s == nil {
		panic("nil iterator")
	}
	return &legacyIter{s: s, self: self}
}

// AsLegacy convert an iterator Shape to a legacy Iterator interface.
func AsLegacy(s IteratorShape) Iterator {
	if it2, ok := s.(IteratorShapeCompat); ok {
		return it2.AsLegacy()
	}
	return NewLegacy(s, nil)
}

var _ IteratorFuture = &legacyIter{}

type legacyIter struct {
	s    IteratorShape
	self Iterator
	scan Scanner
	cont Index
}

func (it *legacyIter) String() string {
	return it.s.String()
}

func (it *legacyIter) AsShape() IteratorShape {
	it.Close()
	return it.s
}

func (it *legacyIter) TagResults(m map[string]Ref) {
	if it.cont != nil && it.scan != nil {
		panic("both iterators are set")
	}
	if it.scan != nil {
		it.scan.TagResults(m)
	} else if it.cont != nil {
		it.cont.TagResults(m)
	}
}

func (it *legacyIter) Result() Ref {
	if it.cont != nil && it.scan != nil {
		panic("both iterators are set")
	}
	if it.scan != nil {
		return it.scan.Result()
	}
	if it.cont != nil {
		return it.cont.Result()
	}
	return nil
}

func (it *legacyIter) Next(ctx context.Context) bool {
	if it.scan == nil {
		if it.cont != nil {
			panic("attempt to set a scan iterator on contains")
		}
		it.scan = it.s.Iterate()
	}
	return it.scan.Next(ctx)
}

func (it *legacyIter) NextPath(ctx context.Context) bool {
	if it.cont != nil && it.scan != nil {
		panic("both iterators are set")
	}
	if it.scan != nil {
		return it.scan.NextPath(ctx)
	}
	if it.cont != nil {
		return it.cont.NextPath(ctx)
	}
	panic("calling NextPath before Next or Contains")
}

func (it *legacyIter) Contains(ctx context.Context, v Ref) bool {
	if it.cont == nil {
		// reset iterator by default
		if it.scan != nil {
			it.scan.Close()
			it.scan = nil
		}
		it.cont = it.s.Lookup()
	}
	return it.cont.Contains(ctx, v)
}

func (it *legacyIter) Err() error {
	if it.scan != nil {
		if err := it.scan.Err(); err != nil {
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
	if it.scan != nil {
		_ = it.scan.Close()
		it.scan = nil
	}
	if it.cont != nil {
		_ = it.cont.Close()
		it.cont = nil
	}
}

func (it *legacyIter) Stats() IteratorStats {
	st, _ := it.s.Stats(context.Background())
	return IteratorStats{
		NextCost:     st.NextCost,
		ContainsCost: st.ContainsCost,
		Size:         st.Size.Size,
		ExactSize:    st.Size.Exact,
	}
}

func (it *legacyIter) Size() (int64, bool) {
	st, _ := it.s.Stats(context.Background())
	return st.Size.Size, st.Size.Exact
}

func (it *legacyIter) Optimize() (Iterator, bool) {
	nit, ok := it.s.Optimize(context.Background())
	if !ok {
		if it.self != nil {
			return it.self, false
		}
		return it, false
	}
	return AsLegacy(nit), true
}

func (it *legacyIter) SubIterators() []Iterator {
	its := it.s.SubIterators()
	out := make([]Iterator, 0, len(its))
	for _, s := range its {
		out = append(out, AsLegacy(s))
	}
	return out
}

func (it *legacyIter) Close() error {
	if it.scan != nil {
		it.scan.Close()
		it.scan = nil
	}
	if it.cont != nil {
		it.cont.Close()
		it.cont = nil
	}
	return nil
}
