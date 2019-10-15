package iterator_test

import (
	"context"
	"fmt"

	"github.com/cayleygraph/cayley/graph"
	. "github.com/cayleygraph/cayley/graph/iterator"
)

// A testing iterator that returns the given values for Next() and Err().
type testIterator struct {
	graph.IteratorShape

	NextVal bool
	ErrVal  error
}

func newTestIterator(next bool, err error) graph.IteratorShape {
	return &testIterator{
		IteratorShape: NewFixed(),
		NextVal:       next,
		ErrVal:        err,
	}
}

func (it *testIterator) Iterate() graph.Scanner {
	return &testIteratorNext{
		Scanner: it.IteratorShape.Iterate(),
		NextVal: it.NextVal,
		ErrVal:  it.ErrVal,
	}
}

func (it *testIterator) Lookup() graph.Index {
	return &testIteratorContains{
		Index:   it.IteratorShape.Lookup(),
		NextVal: it.NextVal,
		ErrVal:  it.ErrVal,
	}
}

// A testing iterator that returns the given values for Next() and Err().
type testIteratorNext struct {
	graph.Scanner

	NextVal bool
	ErrVal  error
}

func (it *testIteratorNext) Next(ctx context.Context) bool {
	return it.NextVal
}

func (it *testIteratorNext) Err() error {
	return it.ErrVal
}

// A testing iterator that returns the given values for Next() and Err().
type testIteratorContains struct {
	graph.Index

	NextVal bool
	ErrVal  error
}

func (it *testIteratorContains) Contains(ctx context.Context, v graph.Ref) bool {
	return it.NextVal
}

func (it *testIteratorContains) Err() error {
	return it.ErrVal
}

type Int64Quad int64

func (v Int64Quad) Key() interface{} { return v }

func (Int64Quad) IsNode() bool { return false }

var _ graph.IteratorShape = &Int64{}

// An All iterator across a range of int64 values, from `max` to `min`.
type Int64 struct {
	node     bool
	max, min int64
}

func (it *Int64) Iterate() graph.Scanner {
	return newInt64Next(it.min, it.max, it.node)
}

func (it *Int64) Lookup() graph.Index {
	return newInt64Contains(it.min, it.max, it.node)
}

// Creates a new Int64 with the given range.
func newInt64(min, max int64, node bool) *Int64 {
	return &Int64{
		node: node,
		min:  min,
		max:  max,
	}
}

func (it *Int64) String() string {
	return fmt.Sprintf("Int64(%d-%d)", it.min, it.max)
}

// No sub-iterators.
func (it *Int64) SubIterators() []graph.IteratorShape {
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

// There's nothing to optimize about this little iterator.
func (it *Int64) Optimize(ctx context.Context) (graph.IteratorShape, bool) { return it, false }

// Stats for an Int64 are simple. Super cheap to do any operation,
// and as big as the range.
func (it *Int64) Stats(ctx context.Context) (graph.IteratorCosts, error) {
	s, exact := it.Size()
	return graph.IteratorCosts{
		ContainsCost: 1,
		NextCost:     1,
		Size: graph.Size{
			Value: s,
			Exact: exact,
		},
	}, nil
}

// An All iterator across a range of int64 values, from `max` to `min`.
type int64Next struct {
	node     bool
	max, min int64
	at       int64
	result   int64
}

// Creates a new Int64 with the given range.
func newInt64Next(min, max int64, node bool) *int64Next {
	return &int64Next{
		node: node,
		min:  min,
		max:  max,
		at:   min,
	}
}

func (it *int64Next) Close() error {
	return nil
}

func (it *int64Next) TagResults(dst map[string]graph.Ref) {}

func (it *int64Next) String() string {
	return fmt.Sprintf("Int64(%d-%d)", it.min, it.max)
}

// Next() on an Int64 all iterator is a simple incrementing counter.
// Return the next integer, and mark it as the result.
func (it *int64Next) Next(ctx context.Context) bool {
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

func (it *int64Next) Err() error {
	return nil
}

func (it *int64Next) toValue(v int64) graph.Ref {
	if it.node {
		return Int64Node(v)
	}
	return Int64Quad(v)
}

func (it *int64Next) Result() graph.Ref {
	return it.toValue(it.result)
}

func (it *int64Next) NextPath(ctx context.Context) bool {
	return false
}

// An All iterator across a range of int64 values, from `max` to `min`.
type int64Contains struct {
	node     bool
	max, min int64
	at       int64
	result   int64
}

// Creates a new Int64 with the given range.
func newInt64Contains(min, max int64, node bool) *int64Contains {
	return &int64Contains{
		node: node,
		min:  min,
		max:  max,
		at:   min,
	}
}

func (it *int64Contains) Close() error {
	return nil
}

func (it *int64Contains) TagResults(dst map[string]graph.Ref) {}

func (it *int64Contains) String() string {
	return fmt.Sprintf("Int64(%d-%d)", it.min, it.max)
}

func (it *int64Contains) Err() error {
	return nil
}

func (it *int64Contains) toValue(v int64) graph.Ref {
	if it.node {
		return Int64Node(v)
	}
	return Int64Quad(v)
}

func (it *int64Contains) Result() graph.Ref {
	return it.toValue(it.result)
}

func (it *int64Contains) NextPath(ctx context.Context) bool {
	return false
}

// No sub-iterators.
func (it *int64Contains) SubIterators() []graph.IteratorShape {
	return nil
}

// Contains() for an Int64 is merely seeing if the passed value is
// within the range, assuming the value is an int64.
func (it *int64Contains) Contains(ctx context.Context, tsv graph.Ref) bool {
	v := valToInt64(tsv)
	if it.min <= v && v <= it.max {
		it.result = v
		return true
	}
	return false
}
