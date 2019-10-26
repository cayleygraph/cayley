package linkedql

import (
	"context"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"
)

// TagsIterator is a result iterator for records consisting of selected tags
// or all the tags in the query.
type TagsIterator struct {
	valueIterator *ValueIterator
	selected      []string
}

// Next implements query.Iterator
func (it *TagsIterator) Next(ctx context.Context) bool {
	return it.valueIterator.Next(ctx)
}

// Result implements query.Iterator
func (it *TagsIterator) Result() interface{} {
	refTags := make(map[string]graph.Ref)
	it.valueIterator.scanner.TagResults(refTags)
	tags := make(map[string]quad.Value)
	if it.selected != nil {
		for _, tag := range it.selected {
			tags[tag] = it.valueIterator.namer.NameOf(refTags[tag])
		}
	} else {
		for tag, ref := range refTags {
			tags[tag] = it.valueIterator.namer.NameOf(ref)
		}
	}
	return tags
}

// Err implements query.Iterator
func (it *TagsIterator) Err() error {
	return it.valueIterator.Err()
}

// Close implements query.Iterator
func (it *TagsIterator) Close() error {
	return it.valueIterator.Close()
}
