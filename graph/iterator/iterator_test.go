package iterator_test

import (
	"context"

	"github.com/cayleygraph/cayley/graph"
	. "github.com/cayleygraph/cayley/graph/iterator"
)

// A testing iterator that returns the given values for Next() and Err().
type testIterator struct {
	*Fixed

	NextVal bool
	ErrVal  error
}

func newTestIterator(next bool, err error) graph.Iterator {
	return &testIterator{
		Fixed:   NewFixed(),
		NextVal: next,
		ErrVal:  err,
	}
}

func (it *testIterator) Next(ctx context.Context) bool {
	return it.NextVal
}

func (it *testIterator) Err() error {
	return it.ErrVal
}
