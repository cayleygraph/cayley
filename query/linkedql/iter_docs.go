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
	qs         graph.QuadStore
	path       *path.Path
	ids        []quad.Value
	scanner    iterator.Scanner
	properties map[quad.Value]map[string][]quad.Value
	current    int
}

// NewDocumentIterator returns a new DocumentIterator for a QuadStore and Path.
func NewDocumentIterator(qs graph.QuadStore, p *path.Path) *DocumentIterator {
	return &DocumentIterator{qs: qs, path: p, current: -1}
}

// Next implements query.Iterator.
func (it *DocumentIterator) Next(ctx context.Context) bool {
	if it.properties == nil {
		it.properties = make(map[quad.Value]map[string][]quad.Value)
		it.scanner = it.path.BuildIterator(ctx).Iterate()
		for it.scanner.Next(ctx) {
			id := it.qs.NameOf(it.scanner.Result())
			it.ids = append(it.ids, id)

			tags := make(map[string]graph.Ref)
			it.scanner.TagResults(tags)

			for k, ref := range tags {
				value := it.qs.NameOf(ref)
				m, ok := it.properties[id]
				if !ok {
					m = make(map[string][]quad.Value)
					it.properties[id] = m
				}
				m[k] = append(m[k], value)
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
