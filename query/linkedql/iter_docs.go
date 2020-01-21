package linkedql

import (
	"context"

	"github.com/cayleygraph/cayley/query"
	"github.com/linkeddata/gojsonld"
)

var _ query.Iterator = (*DocumentIterator)(nil)

// DocumentIterator is an iterator of documents from the graph
type DocumentIterator struct {
	tagsIt  *TagsIterator
	records []interface{}
	current int
}

// NewDocumentIterator returns a new DocumentIterator for a QuadStore and Path.
func NewDocumentIterator(valueIt *ValueIterator) *DocumentIterator {
	tagsIt := &TagsIterator{ValueIt: valueIt, Selected: nil}
	return &DocumentIterator{tagsIt: tagsIt, current: -1}
}

// Next implements query.Iterator.
func (it *DocumentIterator) Next(ctx context.Context) bool {
	if it.records == nil {
		for it.tagsIt.Next(ctx) {
			it.records = append(it.records, it.tagsIt.Result())
		}
	}
	if it.current < len(it.records)-1 {
		it.current++
		return true
	}
	return false
}

// Result implements query.Iterator.
func (it *DocumentIterator) Result() interface{} {
	if it.current >= len(it.records) {
		return nil
	}
	input := interface{}(it.records)
	context := make(map[string]interface{})
	expanded, err := gojsonld.Compact(input, context, gojsonld.NewOptions(""))
	if err != nil {
		panic(err)
	}
	return expanded
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
