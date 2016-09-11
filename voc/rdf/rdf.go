// Package rdf contains constants of the RDF Concepts Vocabulary (RDF)
package rdf

import "github.com/cayleygraph/cayley/voc"

func init() {
	voc.RegisterPrefix(Prefix, NS)
}

const (
	NS     = `http://www.w3.org/1999/02/22-rdf-syntax-ns#`
	Prefix = `rdf:`
)

const (
	// Types

	// The datatype of RDF literals storing fragments of HTML content
	HTML = Prefix + `HTML`
	// The datatype of language-tagged string values
	LangString = Prefix + `langString`
	// The class of plain (i.e. untyped) literal values, as used in RIF and OWL 2
	PlainLiteral = Prefix + `PlainLiteral`
	// The class of RDF properties.
	Property = Prefix + `Property`
	// The class of RDF statements.
	Statement = Prefix + `Statement`

	// Properties

	// The subject is an instance of a class.
	Type = Prefix + `type`
	// Idiomatic property used for structured values.
	Value = Prefix + `value`
	// The subject of the subject RDF statement.
	Subject = Prefix + `subject`
	// The predicate of the subject RDF statement.
	Predicate = Prefix + `predicate`
	// The object of the subject RDF statement.
	Object = Prefix + `object`

	// The class of unordered containers.
	Bag = Prefix + `Bag`
	// The class of ordered containers.
	Seq = Prefix + `Seq`
	// The class of containers of alternatives.
	Alt = Prefix + `Alt`
	// The class of RDF Lists.
	List = Prefix + `List`
	// The empty list, with no items in it. If the rest of a list is nil then the list has no more items in it.
	Nil = Prefix + `nil`
	// The first item in the subject RDF list.
	First = Prefix + `first`
	// The rest of the subject RDF list after the first item.
	Rest = Prefix + `rest`
	// The datatype of XML literal values.
	XMLLiteral = Prefix + `XMLLiteral`
)
