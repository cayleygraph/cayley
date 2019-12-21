package linkedql

import (
	"context"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
)

var _ query.Iterator = (*ValueIterator)(nil)

// ValueIterator is an iterator of values from the graph.
type ValueIterator struct {
	namer   refs.Namer
	path    *path.Path
	scanner iterator.Scanner
}

// NewValueIterator returns a new ValueIterator for a path and namer.
func NewValueIterator(p *path.Path, namer refs.Namer) *ValueIterator {
	return &ValueIterator{namer: namer, path: p}
}

// NewValueIteratorFromPathStep attempts to build a path from PathStep and return a new ValueIterator of it.
// If BuildPath fails returns error.
func NewValueIteratorFromPathStep(step PathStep, qs graph.QuadStore) (*ValueIterator, error) {
	p, err := step.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p, qs), nil
}

// Next implements query.Iterator.
func (it *ValueIterator) Next(ctx context.Context) bool {
	if it.scanner == nil {
		it.scanner = it.path.BuildIterator(ctx).Iterate()
	}
	return it.scanner.Next(ctx)
}

// Value returns the current value
func (it *ValueIterator) Value() quad.Value {
	if it.scanner == nil {
		return nil
	}
	return it.namer.NameOf(it.scanner.Result())
}

// Result implements query.Iterator.
func (it *ValueIterator) Result() interface{} {
	return it.Value()
}

// Err implements query.Iterator.
func (it *ValueIterator) Err() error {
	if it.scanner == nil {
		return nil
	}
	return it.scanner.Err()
}

// Close implements query.Iterator.
func (it *ValueIterator) Close() error {
	if it.scanner == nil {
		return nil
	}
	return it.scanner.Close()
}
