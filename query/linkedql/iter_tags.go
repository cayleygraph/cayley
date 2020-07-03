package linkedql

import (
	"context"
	"fmt"

	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/jsonld"
	"github.com/piprate/json-gold/ld"
)

var _ query.Iterator = (*TagsIterator)(nil)

// TagsIterator is a result iterator for records consisting of selected tags
// or all the tags in the query.
type TagsIterator struct {
	ValueIt   *ValueIterator
	Selected  []string
	ExcludeID bool
	err       error
}

// NewTagsIterator creates a new TagsIterator
func NewTagsIterator(valueIt *ValueIterator, selected []string, excludeID bool) TagsIterator {
	return TagsIterator{
		ValueIt:   valueIt,
		Selected:  selected,
		ExcludeID: excludeID,
		err:       nil,
	}
}

// Next implements query.Iterator.
func (it *TagsIterator) Next(ctx context.Context) bool {
	return it.ValueIt.Next(ctx)
}

func (it *TagsIterator) addQuadFromRef(dataset *ld.RDFDataset, subject ld.Node, tag string, ref refs.Ref) error {
	p := ld.NewIRI(tag)
	o, err := jsonld.ToNode(it.ValueIt.Namer.NameOf(ref))
	if err != nil {
		return err
	}
	q := ld.NewQuad(subject, p, o, "")
	dataset.Graphs["@default"] = append(dataset.Graphs["@default"], q)
	return nil
}

func toSubject(namer refs.Namer, result refs.Ref) (ld.Node, error) {
	v := namer.NameOf(result)
	id, ok := v.(quad.Identifier)
	if !ok {
		return nil, fmt.Errorf("Expected subject to be an entity identifier but instead received: %v", v)
	}
	return jsonld.ToNode(id)
}

func (it *TagsIterator) addResultsToDataset(dataset *ld.RDFDataset, result refs.Ref) error {
	s, err := toSubject(it.ValueIt.Namer, result)
	if err != nil {
		return err
	}

	refTags := make(map[string]refs.Ref)

	it.ValueIt.scanner.TagResults(refTags)

	if len(it.Selected) == 0 {
		for tag, ref := range refTags {
			it.addQuadFromRef(dataset, s, tag, ref)
		}
	} else {
		for _, tag := range it.Selected {
			it.addQuadFromRef(dataset, s, tag, refTags[tag])
		}
	}

	return nil
}

// Result implements query.Iterator.
func (it *TagsIterator) Result() interface{} {
	// FIXME(iddan): only convert when collation is JSON/JSON-LD, leave as Ref otherwise
	r := it.ValueIt.scanner.Result()
	if r == nil {
		return nil
	}
	d := ld.NewRDFDataset()
	err := it.addResultsToDataset(d, r)
	if err != nil {
		it.err = err
		return nil
	}
	doc, err := singleDocumentFromRDF(d)
	if err != nil {
		it.err = err
		return nil
	}
	if !it.ExcludeID {
		m := doc.(map[string]interface{})
		delete(m, "@id")
		return m
	}
	return doc
}

// Err implements query.Iterator.
func (it *TagsIterator) Err() error {
	if it.err != nil {
		return it.err
	}
	return it.ValueIt.Err()
}

// Close implements query.Iterator.
func (it *TagsIterator) Close() error {
	return it.ValueIt.Close()
}
