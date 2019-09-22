package iterator_test

import (
	"context"
	"fmt"

	"github.com/cayleygraph/cayley/graph"
	. "github.com/cayleygraph/cayley/graph/iterator"
)

// A testing iterator that returns the given values for Next() and Err().
type testIterator struct {
	graph.Iterator

	NextVal bool
	ErrVal  error
}

func newTestIterator(next bool, err error) graph.Iterator {
	return &testIterator{
		Iterator: NewFixed(),
		NextVal:  next,
		ErrVal:   err,
	}
}

func (it *testIterator) Next(ctx context.Context) bool {
	return it.NextVal
}

func (it *testIterator) Err() error {
	return it.ErrVal
}

type Int64Quad int64

func (v Int64Quad) Key() interface{} { return v }

func (Int64Quad) IsNode() bool { return false }

var _ graph.Iterator = &Int64{}

// An All iterator across a range of int64 values, from `max` to `min`.
type Int64 struct {
	node     bool
	max, min int64
	at       int64
	result   int64
	runstats graph.IteratorStats
}

// Creates a new Int64 with the given range.
func newInt64(min, max int64, node bool) *Int64 {
	return &Int64{
		node: node,
		min:  min,
		max:  max,
		at:   min,
	}
}

// Start back at the beginning
func (it *Int64) Reset() {
	it.at = it.min
}

func (it *Int64) Close() error {
	return nil
}

func (it *Int64) TagResults(dst map[string]graph.Ref) {}

func (it *Int64) String() string {
	return fmt.Sprintf("Int64(%d-%d)", it.min, it.max)
}

// Next() on an Int64 all iterator is a simple incrementing counter.
// Return the next integer, and mark it as the result.
func (it *Int64) Next(ctx context.Context) bool {
	it.runstats.Next += 1
	if it.at == -1 {
		return false
	}
	val := it.at
	it.at = it.at + 1
	if it.at > it.max {
		it.at = -1
	}
	it.result = val
	return true
}

func (it *Int64) Err() error {
	return nil
}

func (it *Int64) toValue(v int64) graph.Ref {
	if it.node {
		return Int64Node(v)
	}
	return Int64Quad(v)
}

func (it *Int64) Result() graph.Ref {
	return it.toValue(it.result)
}

func (it *Int64) NextPath(ctx context.Context) bool {
	return false
}

// No sub-iterators.
func (it *Int64) SubIterators() []graph.Iterator {
	return nil
}

// The number of elements in an Int64 is the size of the range.
// The size is exact.
func (it *Int64) Size() (int64, bool) {
	sz := (it.max - it.min) + 1
	return sz, true
}

func valToInt64(v graph.Ref) int64 {
	if v, ok := v.(Int64Node); ok {
		return int64(v)
	}
	return int64(v.(Int64Quad))
}

// Contains() for an Int64 is merely seeing if the passed value is
// within the range, assuming the value is an int64.
func (it *Int64) Contains(ctx context.Context, tsv graph.Ref) bool {
	it.runstats.Contains += 1
	v := valToInt64(tsv)
	if it.min <= v && v <= it.max {
		it.result = v
		return true
	}
	return false
}

// There's nothing to optimize about this little iterator.
func (it *Int64) Optimize() (graph.Iterator, bool) { return it, false }

// Stats for an Int64 are simple. Super cheap to do any operation,
// and as big as the range.
func (it *Int64) Stats() graph.IteratorStats {
	s, exact := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     1,
		Size:         s,
		ExactSize:    exact,
		Next:         it.runstats.Next,
		Contains:     it.runstats.Contains,
	}
}
