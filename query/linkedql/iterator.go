package linkedql

import (
	"context"

	"github.com/cayleygraph/cayley/graph"
)

type Iterator struct {
	namer   graph.Namer
	scanner graph.Scanner
}

func (it *Iterator) Next(ctx context.Context) bool {
	return it.scanner.Next(ctx)
}

func (it *Iterator) Result() interface{} {
	return it.namer.NameOf(it.scanner.Result())
}

func (it *Iterator) Err() error {
	return it.scanner.Err()
}

func (it *Iterator) Close() error {
	return it.scanner.Close()
}
