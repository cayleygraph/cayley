package linkedql

import (
	"context"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"
)

type TagArrayIterator struct {
	valueIterator *ValueIterator
}

func (it *TagArrayIterator) Next(ctx context.Context) bool {
	return it.valueIterator.Next(ctx)
}

func (it *TagArrayIterator) Result() interface{} {
	refTags := make(map[string]graph.Ref)
	it.valueIterator.Scanner.TagResults(refTags)
	tags := make(map[string]quad.Value)
	for tag, ref := range refTags {
		tags[tag] = it.valueIterator.namer.NameOf(ref)
	}
	// TODO(iddan): collect tags of current iteration
	return tags
}

func (it *TagArrayIterator) Err() error {
	return it.valueIterator.Err()
}

func (it *TagArrayIterator) Close() error {
	return it.valueIterator.Close()
}
