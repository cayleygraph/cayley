package linkedql

import (
	"context"

	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/jsonld"
)

var _ query.Iterator = (*TagsIterator)(nil)

type tags = map[string]interface{}

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

// FIXME(iddan): don't cast to string when collation is Raw
func stringifyID(id quad.Identifier) string {
	var sid string
	switch val := id.(type) {
	case quad.IRI:
		sid = string(val)
	case quad.BNode:
		sid = val.String()
	}
	return sid
}

// FIXME(iddan): only convert when collation is JSON/JSON-LD, leave as Ref otherwise
func fromValue(value quad.Value) interface{} {
	v := jsonld.FromValue(value)
	if m, ok := v.(map[string]string); ok {
		n := make(map[string]interface{})
		for k, v := range m {
			n[k] = v
		}
		return n
	}
	return v
}

func getID(it *TagsIterator) (string, bool) {
	scanner := it.ValueIt.scanner
	identifier, ok := it.ValueIt.getName(scanner.Result()).(quad.Identifier)
	if ok {
		return stringifyID(identifier), true
	}
	return "", false
}

func (it *TagsIterator) getTags() tags {
	scanner := it.ValueIt.scanner
	refTags := make(map[string]refs.Ref)
	scanner.TagResults(refTags)
	t := make(tags)
	if it.Selected != nil {
		for _, tag := range it.Selected {
			if tag == "@id" {
				id, ok := getID(it)
				if ok {
					t[tag] = id
				}
				continue
			}
			t[tag] = fromValue(it.ValueIt.getName(refTags[tag]))
		}
	} else {
		id, ok := getID(it)
		if ok {
			t["@id"] = id
		}
		for tag, ref := range refTags {
			t[tag] = fromValue(it.ValueIt.getName(ref))
		}
	}

	return t
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
