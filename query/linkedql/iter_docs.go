package linkedql

import (
	"context"

	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
	"github.com/piprate/json-gold/ld"
)

var (
	_ query.Iterator = (*DocumentIterator)(nil)
)

// DocumentIterator is an iterator of documents from the graph
type DocumentIterator struct {
	quadIt    *QuadIterator
	quads     []quad.Quad
	err       error
	exhausted bool
}

// NewDocumentIterator constructs a DocumentIterator for a Namer and Path
func NewDocumentIterator(namer refs.Namer, path *path.Path) *DocumentIterator {
	quadIt := NewQuadIterator(namer, path, nil)
	return NewDocumentIteratorFromQuadIterator(quadIt)
}

// NewDocumentIteratorFromQuadIterator constructs DocumentIterator from a QuadIterator
func NewDocumentIteratorFromQuadIterator(quadIt *QuadIterator) *DocumentIterator {
	return &DocumentIterator{quadIt: quadIt, exhausted: false}
}

func (it *DocumentIterator) getQuads(ctx context.Context) ([]quad.Quad, error) {
	var allQuads []quad.Quad
	for it.quadIt.Next(ctx) {
		quads, err := it.quadIt.resultQuads()
		if err != nil {
			return nil, err
		}
		allQuads = append(allQuads, quads...)
	}
	return allQuads, nil
}

// Next implements query.Iterator.
func (it *DocumentIterator) Next(ctx context.Context) bool {
	if !it.exhausted {
		quads, err := it.getQuads(ctx)
		if err != nil {
			it.err = err
		} else {
			it.quads = quads
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
	d, err := quadsToDataset(it.quads)
	if err != nil {
		it.err = err
	}
	c, err := datasetToCompact(d, context, opts)
	if err != nil {
		it.err = err
	}
	return c
}

// Err implements query.Iterator.
func (it *DocumentIterator) Err() error {
	if it.err != nil {
		return it.err
	}
	return it.quadIt.Err()
}

// Close implements query.Iterator.
func (it *DocumentIterator) Close() error {
	if it.quadIt == nil {
		return nil
	}
	return it.quadIt.Close()
}
