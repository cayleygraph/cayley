package linkedql

import (
	"context"

	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
)

var _ query.Iterator = (*DocumentIterator)(nil)

type document = map[string]interface{}
type properties = map[string][]interface{}
type idToProperties = map[quad.Value]properties

// DocumentIterator is an iterator of documents from the graph
type DocumentIterator struct {
	tagsIt     *TagsIterator
	ids        []quad.Value
	properties idToProperties
	current    int
}

// NewDocumentIterator returns a new DocumentIterator for a QuadStore and Path.
func NewDocumentIterator(qs refs.Namer, p *path.Path) *DocumentIterator {
	tagsIt := &TagsIterator{valueIt: NewValueIterator(p, qs), selected: nil}
	return &DocumentIterator{tagsIt: tagsIt, current: -1}
}

// Next implements query.Iterator.
func (it *DocumentIterator) Next(ctx context.Context) bool {
	if it.properties == nil {
		it.properties = make(idToProperties)
		for it.Next(ctx) {
			id := it.tagsIt.valueIt.Value()
			tags := it.tagsIt.getTags()
			it.ids = append(it.ids, id)
			for k, v := range tags {
				m, ok := it.properties[id]
				if !ok {
					m = make(properties)
					it.properties[id] = m
				}
				m[k] = append(m[k], v)
			}
		}
	}
	if it.current < len(it.ids)-1 {
		it.current++
		return true
	}
	return false
}

// Result implements query.Iterator.
func (it *DocumentIterator) Result() interface{} {
	if it.current >= len(it.ids) {
		return nil
	}
	id := it.ids[it.current]
	var sid string
	switch val := id.(type) {
	case quad.IRI:
		sid = string(val)
	case quad.BNode:
		sid = val.String()
	}
	d := document{
		"@id": sid,
	}
	for k, v := range it.properties[id] {
		d[k] = v
	}
	return d
}

// Err implements query.Iterator.
func (it *DocumentIterator) Err() error {
	if it.tagsIt == nil {
		return nil
	}
	return it.tagsIt.Err()
}

// Close implements query.Iterator.
func (it *DocumentIterator) Close() error {
	if it.tagsIt == nil {
		return nil
	}
	return it.tagsIt.Close()
}
