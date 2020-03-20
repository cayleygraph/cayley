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
	ValueIt  *ValueIterator
	Selected []string
	err      error
}

// NewTagsIterator creates a new TagsIterator
func NewTagsIterator(valueIt *ValueIterator, selected []string) TagsIterator {
	return TagsIterator{
		ValueIt:  valueIt,
		Selected: selected,
		err:      nil,
	}
}

// Next implements query.Iterator.
func (it *TagsIterator) Next(ctx context.Context) bool {
	return it.ValueIt.Next(ctx)
}

// FIXME(iddan): don't cast to string when collation is Raw
func stringifyID(id quad.Identifier) string {
	var sid string
	switch val := id.(type) {
	case quad.IRI:
		sid = string(val)
	case quad.BNode:
		sid = val.String()
	}
	return sid
}

// FIXME(iddan): only convert when collation is JSON/JSON-LD, leave as Ref otherwise
func fromValue(value quad.Value) ld.Node {
	switch v := value.(type) {
	case quad.IRI:
		return ld.NewIRI(string(v))
	case quad.BNode:
		return ld.NewBlankNode(string(v))
	case quad.String:
		return ld.NewLiteral(string(v), "", "")
	case quad.TypedString:
		return ld.NewLiteral(string(v.Value), string(v.Type), "")
	case quad.LangString:
		return ld.NewLiteral(string(v.Value), "", v.Lang)
	}
	panic(fmt.Errorf("Can not convert %v to ld.Node", value))
}

func getSubject(it *TagsIterator) ld.Node {
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

func (it *TagsIterator) getTags() (interface{}, error) {
	d := ld.NewRDFDataset()
	s := getSubject(it)
	scanner := it.ValueIt.scanner
	refTags := make(map[string]refs.Ref)

	scanner.TagResults(refTags)

	if it.Selected != nil {
		for _, tag := range it.Selected {
			p := ld.NewIRI(tag)
			o := fromValue(it.ValueIt.getName(refTags[tag]))
			q := ld.NewQuad(s, p, o, "")
			d.Graphs["@default"] = append(d.Graphs["@default"], q)
		}
		r, err := fromRDF(d)
		d := r.(map[string]interface{})
		if err != nil {
			return nil, err
		}
		delete(d, "@id")
		return d, nil
	}

	for tag, ref := range refTags {
		p := ld.NewIRI(tag)
		o := fromValue(it.ValueIt.getName(ref))
		q := ld.NewQuad(s, p, o, "")
		d.Graphs["@default"] = append(d.Graphs["@default"], q)
	}
	return fromRDF(d)
}

// Result implements query.Iterator.
func (it *TagsIterator) Result() interface{} {
	tags, err := it.getTags()
	if err != nil {
		it.err = err
		return nil
	}
	return tags
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
