package linkedql

import (
	"context"
	"fmt"

	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/quad"
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

// FIXME(iddan): only convert when collation is JSON/JSON-LD, leave as Ref otherwise
func fromValue(value quad.Value) (ld.Node, error) {
	switch v := value.(type) {
	case quad.IRI:
		return ld.NewIRI(string(v)), nil
	case quad.BNode:
		return ld.NewBlankNode(string(v)), nil
	case quad.String:
		return ld.NewLiteral(string(v), "", ""), nil
	case quad.TypedString:
		return ld.NewLiteral(string(v.Value), string(v.Type), ""), nil
	case quad.LangString:
		return ld.NewLiteral(string(v.Value), "", v.Lang), nil
	default:
		return nil, fmt.Errorf("Can not convert %v to ld.Node", value)
	}
}

func getSubject(it *TagsIterator) (ld.Node, error) {
	scanner := it.ValueIt.scanner
	identifier := it.ValueIt.getName(scanner.Result()).(quad.Identifier)
	return fromValue(identifier)
}

func fromRDF(dataset *ld.RDFDataset) (interface{}, error) {
	api := ld.NewJsonLdApi()
	opts := ld.NewJsonLdOptions("")
	documents, err := api.FromRDF(dataset, opts)
	if err != nil {
		return nil, err
	}
	if len(documents) != 1 {
		return nil, fmt.Errorf("Unexpected number of documents")
	}
	return documents[0], nil
}

func (it *TagsIterator) createQuad(subject ld.Node, tag string, ref refs.Ref) (*ld.Quad, error) {
	p := ld.NewIRI(tag)
	o, err := fromValue(it.ValueIt.getName(ref))
	if err != nil {
		return nil, err
	}
	return ld.NewQuad(subject, p, o, ""), nil
}

func (it *TagsIterator) getDataset() (*ld.RDFDataset, error) {
	d := ld.NewRDFDataset()

	s, err := getSubject(it)
	if err != nil {
		return nil, err
	}

	scanner := it.ValueIt.scanner
	refTags := make(map[string]refs.Ref)

	scanner.TagResults(refTags)

	if len(it.Selected) == 0 {
		for tag, ref := range refTags {
			q, err := it.createQuad(s, tag, ref)
			if err != nil {
				return nil, err
			}
			d.Graphs["@default"] = append(d.Graphs["@default"], q)
		}
	} else {
		for _, tag := range it.Selected {
			q, err := it.createQuad(s, tag, refTags[tag])
			if err != nil {
				return nil, err
			}
			d.Graphs["@default"] = append(d.Graphs["@default"], q)
		}
	}

	return d, nil
}

// Result implements query.Iterator.
func (it *TagsIterator) Result() interface{} {
	d, err := it.getDataset()
	if err != nil {
		it.err = err
		return nil
	}
	r, err := fromRDF(d)
	if err != nil {
		it.err = err
		return nil
	}
	m := r.(map[string]interface{})
	if !it.ExcludeID {
		delete(m, "@id")
	}
	return m
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
