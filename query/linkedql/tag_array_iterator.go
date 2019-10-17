package linkedql

import (
	"context"

	"github.com/cayleygraph/quad"
)

type TagArrayIterator struct {
	valueIterator *ValueIterator
}

func (it *TagArrayIterator) Next(ctx context.Context) bool {
	return it.valueIterator.Next(ctx)
}

func (it *TagArrayIterator) Result() interface{} {
	tags := make(map[string]quad.Value)
	// TODO(iddan): collect tags of current iteration
	return tags
}

func (it *TagArrayIterator) Err() error {
	return it.valueIterator.Err()
}

func (it *TagArrayIterator) Close() error {
	return it.valueIterator.Close()
}
