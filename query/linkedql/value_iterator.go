package linkedql

import (
	"context"

	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/cayley/query/path"
)

// ValueIterator is an iterator of values from the graph
type ValueIterator struct {
	namer   refs.Namer
	path    *path.Path
	scanner iterator.Scanner
}

// NewValueIterator returns a new ValueIterator for a path and namer
func NewValueIterator(p *path.Path, namer refs.Namer) *ValueIterator {
	return &ValueIterator{namer, p, nil}
}

// Next implements query.Iterator
func (it *ValueIterator) Next(ctx context.Context) bool {
	if it.scanner == nil {
		it.scanner = it.path.BuildIterator(ctx).Iterate()
	}
	return it.scanner.Next(ctx)
}

// Result implements query.Iterator
func (it *ValueIterator) Result() interface{} {
	if it.scanner == nil {
		return nil
	}
	return it.namer.NameOf(it.scanner.Result())
}

// Err implements query.Iterator
func (it *ValueIterator) Err() error {
	if it.scanner == nil {
		return nil
	}
	return it.scanner.Err()
}

// Close implements query.Iterator
func (it *ValueIterator) Close() error {
	if it.scanner == nil {
		return nil
	}
	return it.scanner.Close()
}
