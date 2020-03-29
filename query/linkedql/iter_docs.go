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
		t, err := it.tagsIt.getDataset()
		if err != nil {
			return nil, err
		}
		for g, qs := range t.Graphs {
			d.Graphs[g] = append(d.Graphs[g], qs...)
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
	api := ld.NewJsonLdApi()
	proc := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	d, err := api.FromRDF(it.dataset, options)
	if err != nil {
		it.err = err
		return nil
	}
	c, err := proc.Compact(d, context, options)
	if err != nil {
		it.err = err
		return nil
	}
	return c
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
