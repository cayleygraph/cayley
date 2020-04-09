package linkedql

import (
	"context"
	"fmt"

	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/jsonld"
	"github.com/piprate/json-gold/ld"
)

var _ query.Iterator = (*QuadIterator)(nil)

// QuadIterator is an iterator that returns quads of the results of a query
type QuadIterator struct {
	Namer      refs.Namer
	Path       *path.Path
	Properties PropertyIRIs
	scanner    iterator.Scanner
	err        error
}

// NewQuadIterator creates a new QuadIterator
func NewQuadIterator(namer refs.Namer, path *path.Path, properties PropertyIRIs) *QuadIterator {
	return &QuadIterator{
		Namer:      namer,
		Path:       path,
		Properties: properties,
		scanner:    nil,
		err:        nil,
	}
}

// createScanner creates a scanner for the iterator if not created before
func (it *QuadIterator) createScanner(ctx context.Context) {
	if it.scanner == nil {
		it.scanner = it.Path.BuildIterator(ctx).Iterate()
	}
}

// Next implements query.Iterator.
func (it *QuadIterator) Next(ctx context.Context) bool {
	it.createScanner(ctx)
	return it.scanner.Next(ctx)
}

func toSubject(namer refs.Namer, result refs.Ref) (quad.Identifier, error) {
	v := namer.NameOf(result)
	id, ok := v.(quad.Identifier)
	if !ok {
		return nil, fmt.Errorf("Expected subject to be an entity identifier but instead received: %v", v)
	}
	return id, nil
}

func toQuad(namer refs.Namer, subject quad.Value, tag string, ref refs.Ref) quad.Quad {
	p := quad.IRI(tag)
	o := namer.NameOf(ref)
	return quad.Quad{Subject: subject, Predicate: p, Object: o}
}

func (it *QuadIterator) resultQuads() ([]quad.Quad, error) {
	r := it.scanner.Result()
	if r == nil {
		return nil, nil
	}
	s, err := toSubject(it.Namer, r)
	if err != nil {
		return nil, err
	}
	var quads []quad.Quad
	refTags := make(map[string]refs.Ref)
	it.scanner.TagResults(refTags)
	if len(refTags) == 0 {
		quads = append(quads, MakeSingleEntityQuad(s))
	} else if len(it.Properties) == 0 {
		for tag, ref := range refTags {
			p := quad.IRI(tag)
			o := it.Namer.NameOf(ref)
			q := quad.Quad{Subject: s, Predicate: p, Object: o}
			quads = append(quads, q)
		}
	} else {
		for _, p := range it.Properties {
			ref := refTags[string(p)]
			o := it.Namer.NameOf(ref)
			q := quad.Quad{Subject: s, Predicate: quad.IRI(p), Object: o}
			quads = append(quads, q)
		}
	}
	return quads, nil
}

func quadsToDataset(quads []quad.Quad) (*ld.RDFDataset, error) {
	d := ld.NewRDFDataset()
	// TODO(iddan): reuse code from quad
	for _, q := range quads {
		var graph string
		if q.Label != nil {
			graph = q.Label.String()
		} else {
			graph = "@default"
		}
		s, err := jsonld.ToNode(q.Subject)
		if err != nil {
			return nil, err
		}
		p, err := jsonld.ToNode(q.Predicate)
		if err != nil {
			return nil, err
		}
		o, err := jsonld.ToNode(q.Object)
		if err != nil {
			return nil, err
		}
		d.Graphs[graph] = append(d.Graphs[graph], ld.NewQuad(
			s,
			p,
			o,
			graph,
		))
	}
	return d, nil
}

func (it *QuadIterator) resultJSONLD() (interface{}, error) {
	quads, err := it.resultQuads()
	if err != nil {
		return nil, err
	}
	d, err := quadsToDataset(quads)
	if err != nil {
		return nil, err
	}
	doc, err := singleDocumentFromRDF(d)
	if err != nil {
		return nil, err
	}
	return doc, nil
}

// Result implements quad.Iterator
func (it *QuadIterator) Result() interface{} {
	r, err := it.resultJSONLD()
	if err != nil {
		it.err = err
	}
	return r
}

// Err implements query.Iterator.
func (it *QuadIterator) Err() error {
	if it.err != nil {
		return it.err
	}
	return it.scanner.Err()
}

// Close implements query.Iterator.
func (it *QuadIterator) Close() error {
	return it.scanner.Close()
}
