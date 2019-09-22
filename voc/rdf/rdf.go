// Package rdf is deprecated. Use github.com/cayleygraph/quad/voc/rdf.
package rdf

import (
	"github.com/cayleygraph/quad/voc/rdf"
)

const (
	NS     = rdf.NS
	Prefix = rdf.Prefix
)

const (
	// Types

	// The datatype of RDF literals storing fragments of HTML content
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdf package instead.
	HTML = rdf.HTML
	// The datatype of language-tagged string values
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdf package instead.
	LangString = rdf.LangString
	// The class of plain (i.e. untyped) literal values, as used in RIF and OWL 2
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdf package instead.
	PlainLiteral = rdf.PlainLiteral
	// The class of RDF properties.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdf package instead.
	Property = rdf.Property
	// The class of RDF statements.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdf package instead.
	Statement = rdf.Statement

	// Properties

	// The subject is an instance of a class.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdf package instead.
	Type = rdf.Type
	// Idiomatic property used for structured values.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdf package instead.
	Value = rdf.Value
	// The subject of the subject RDF statement.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdf package instead.
	Subject = rdf.Subject
	// The predicate of the subject RDF statement.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdf package instead.
	Predicate = rdf.Predicate
	// The object of the subject RDF statement.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdf package instead.
	Object = rdf.Object

	// The class of unordered containers.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdf package instead.
	Bag = rdf.Bag
	// The class of ordered containers.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdf package instead.
	Seq = rdf.Seq
	// The class of containers of alternatives.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdf package instead.
	Alt = rdf.Alt
	// The class of RDF Lists.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdf package instead.
	List = rdf.List
	// The empty list, with no items in it. If the rest of a list is nil then the list has no more items in it.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdf package instead.
	Nil = rdf.Nil
	// The first item in the subject RDF list.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdf package instead.
	First = rdf.First
	// The rest of the subject RDF list after the first item.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdf package instead.
	Rest = rdf.Rest
	// The datatype of XML literal values.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdf package instead.
	XMLLiteral = rdf.XMLLiteral
)
