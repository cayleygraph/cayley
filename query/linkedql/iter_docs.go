package linkedql

import (
	"context"

	"github.com/cayleygraph/cayley/query"
	"github.com/piprate/json-gold/ld"
)

var (
	_ query.Iterator = (*DocumentIterator)(nil)
)

// DocumentIterator is an iterator of documents from the graph
type DocumentIterator struct {
	tagsIt    *TagsIterator
	dataset   *ld.RDFDataset
	err       error
	exhausted bool
}

// NewDocumentIterator returns a new DocumentIterator for a QuadStore and Path.
func NewDocumentIterator(valueIt *ValueIterator) *DocumentIterator {
	tagsIt := &TagsIterator{ValueIt: valueIt, Selected: nil}
	return &DocumentIterator{tagsIt: tagsIt, exhausted: false}
}

func (it *DocumentIterator) getDataset(ctx context.Context) (*ld.RDFDataset, error) {
	d := ld.NewRDFDataset()
	for it.tagsIt.Next(ctx) {
		r := it.tagsIt.ValueIt.scanner.Result()
		if err := it.tagsIt.Err(); err != nil {
			if err != nil {
				return nil, err
			}
		}
		if r == nil {
			continue
		}
		err := it.tagsIt.addResultsToDataset(d, r)
		if err != nil {
			return nil, err
		}
	}
	return d, nil
}

// Next implements query.Iterator.
func (it *DocumentIterator) Next(ctx context.Context) bool {
	if !it.exhausted {
		d, err := it.getDataset(ctx)
		if err != nil {
			it.err = err
		} else {
			it.dataset = d
		}
		it.exhausted = true
		return true
	}
	return false
}

// Result implements query.Iterator.
func (it *DocumentIterator) Result() interface{} {
	context := make(map[string]interface{})
	opts := ld.NewJsonLdOptions("")
	c, err := datasetToCompact(it.dataset, context, opts)
	if err != nil {
		it.err = err
	}
	return c
}

// Err implements query.Iterator.
func (it *DocumentIterator) Err() error {
	if it.tagsIt == nil {
		return nil
	}
	if it.err != nil {
		return it.err
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
