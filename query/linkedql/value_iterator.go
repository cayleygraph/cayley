package linkedql

import (
	"context"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query/path"
)

type ValueIterator struct {
	namer   graph.Namer
	path    *path.Path
	Scanner graph.Scanner
}

func NewValueIterator(p *path.Path, namer graph.Namer) *ValueIterator {
	return &ValueIterator{namer, p, nil}
}

func (it *ValueIterator) Next(ctx context.Context) bool {
	if it.Scanner == nil {
		it.Scanner = it.path.BuildIterator().Iterate()
	}
	return it.Scanner.Next(ctx)
}

func (it *ValueIterator) Result() interface{} {
	if it.Scanner == nil {
		return nil
	}
	return it.namer.NameOf(it.Scanner.Result())
}

func (it *ValueIterator) Err() error {
	if it.Scanner == nil {
		return nil
	}
	return it.Scanner.Err()
}

func (it *ValueIterator) Close() error {
	if it.Scanner == nil {
		return nil
	}
	return it.Scanner.Close()
}
