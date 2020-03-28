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
	ValueIt  *ValueIterator
	Selected []string
}

// Next implements query.Iterator.
func (it *TagsIterator) Next(ctx context.Context) bool {
	return it.ValueIt.Next(ctx)
}

func (it *TagsIterator) getTags() map[string]interface{} {
	refTags := make(map[string]refs.Ref)
	it.ValueIt.scanner.TagResults(refTags)

	tags := make(map[string]interface{})
	// FIXME(iddan): only convert when collation is JSON/JSON-LD, leave as Ref otherwise
	if it.Selected != nil {
		for _, tag := range it.Selected {
			tags[tag] = jsonld.FromValue(it.ValueIt.getName(refTags[tag]))
		}
	} else {
		for tag, ref := range refTags {
			tags[tag] = jsonld.FromValue(it.ValueIt.getName(ref))
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
	return it.ValueIt.Err()
}

// Close implements query.Iterator.
func (it *TagsIterator) Close() error {
	return it.ValueIt.Close()
}
