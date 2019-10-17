package linkedql

import (
	"context"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query/path"
)

type ValueIterator struct {
	namer   graph.Namer
	path    *path.Path
	scanner graph.Scanner
}

func NewValueIterator(p *path.Path, namer graph.Namer) *ValueIterator {
	return &ValueIterator{namer, p, nil}
}

func (it *ValueIterator) Next(ctx context.Context) bool {
	if it.scanner == nil {
		it.scanner = it.path.BuildIterator().Iterate()
	}
	return it.scanner.Next(ctx)
}

func (it *ValueIterator) Result() interface{} {
	if it.scanner == nil {
		return nil
	}
	return it.namer.NameOf(it.scanner.Result())
}

func (it *ValueIterator) Err() error {
	if it.scanner == nil {
		return nil
	}
	return it.scanner.Err()
}

func (it *ValueIterator) Close() error {
	if it.scanner == nil {
		return nil
	}
	return it.scanner.Close()
}
