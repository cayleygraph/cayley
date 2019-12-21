package linkedql

import (
	"context"

	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/quad/jsonld"
)

var _ query.Iterator = (*TagsIterator)(nil)

// TagsIterator is a result iterator for records consisting of selected tags
// or all the tags in the query.
type TagsIterator struct {
	valueIt  *ValueIterator
	selected []string
}

// Next implements query.Iterator.
func (it *TagsIterator) Next(ctx context.Context) bool {
	return it.valueIt.Next(ctx)
}

func (it *TagsIterator) getTags() map[string]interface{} {
	refTags := make(map[string]refs.Ref)
	it.valueIt.scanner.TagResults(refTags)

	tags := make(map[string]interface{})
	if it.selected != nil {
		for _, tag := range it.selected {
			tags[tag] = jsonld.FromValue(it.valueIt.getName(refTags[tag]))
		}
	} else {
		for tag, ref := range refTags {
			tags[tag] = jsonld.FromValue(it.valueIt.getName(ref))
		}
	}

	return tags
}

// Result implements query.Iterator.
func (it *TagsIterator) Result() interface{} {
	return it.getTags()
}

// Err implements query.Iterator.
func (it *TagsIterator) Err() error {
	return it.valueIt.Err()
}

// Close implements query.Iterator.
func (it *TagsIterator) Close() error {
	return it.valueIt.Close()
}
