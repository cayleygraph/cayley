package linkedql

import (
	"context"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
)

type document = map[string]interface{}

// DocumentIterator is an iterator of documents from the graph
type DocumentIterator struct {
	qs                 graph.QuadStore
	path               *path.Path
	entities           []quad.Value
	scanner            iterator.Scanner
	entityToProperties map[quad.Value]map[string][]quad.Value
	current            int
}

// NewDocumentIterator returns a new DocumentIterator for a QuadStore and Path.
func NewDocumentIterator(qs graph.QuadStore, p *path.Path) *DocumentIterator {
	return &DocumentIterator{qs: qs, path: p, current: -1}
}

// Next implements query.Iterator.
func (it *DocumentIterator) Next(ctx context.Context) bool {
	if it.entityToProperties == nil {
		it.entityToProperties = make(map[quad.Value]map[string][]quad.Value)
		it.scanner = it.path.BuildIterator(ctx).Iterate()
		for it.scanner.Next(ctx) {
			result := it.scanner.Result()
			entity := it.qs.NameOf(result)
			it.entities = append(it.entities, entity)
			refTags := make(map[string]graph.Ref)
			it.scanner.TagResults(refTags)
			for tag, ref := range refTags {
				value := it.qs.NameOf(ref)
				if properties, ok := it.entityToProperties[entity]; ok {
					properties[tag] = append(properties[tag], value)
				} else {
					it.entityToProperties[entity] = map[string][]quad.Value{tag: {value}}
				}
			}
		}
	}
	if it.current < len(it.entities)-1 {
		it.current++
		return true
	}
	return false
}

// Result implements query.Iterator.
func (it *DocumentIterator) Result() interface{} {
	if it.current == len(it.entities) {
		return nil
	}
	entity := it.entities[it.current]
	var id string
	if iri, ok := entity.(quad.IRI); ok {
		id = string(iri)
	}
	if bnode, ok := entity.(quad.BNode); ok {
		id = bnode.String()
	}
	d := document{
		"@id": id,
	}
	for property, values := range it.entityToProperties[entity] {
		d[property] = values
	}
	return d
}

// Err implements query.Iterator.
func (it *DocumentIterator) Err() error {
	if it.scanner == nil {
		return nil
	}
	return it.scanner.Err()
}

// Close implements query.Iterator.
func (it *DocumentIterator) Close() error {
	if it.scanner == nil {
		return nil
	}
	return it.scanner.Close()
}
