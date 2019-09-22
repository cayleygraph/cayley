package schema

import (
	"github.com/cayleygraph/quad"
	_ "github.com/cayleygraph/quad/voc/rdf"
	_ "github.com/cayleygraph/quad/voc/rdfs"
	"github.com/cayleygraph/quad/voc/schema"
)

func init() {
	RegisterType(quad.IRI(schema.Class), Class{})
	RegisterType(quad.IRI(schema.Property), Property{})
}

type Object struct {
	ID quad.IRI `quad:"@id"`

	Label   string `quad:"rdfs:label,optional"`
	Comment string `quad:"rdfs:comment,optional"`

	Name        string `quad:"schema:name,optional"`
	Description string `quad:"schema:description,optional"`
}

type Property struct {
	Object
	InverseOf    quad.IRI   `quad:"schema:inverseOf,optional"`
	SupersededBy []quad.IRI `quad:"schema:supersededBy,optional"`
	Expects      []quad.IRI `quad:"schema:rangeIncludes"`
}

type Class struct {
	Object
	Properties   []Property `quad:"schema:domainIncludes < *,optional"`
	SupersededBy []quad.IRI `quad:"schema:supersededBy,optional"`
	Extends      []quad.IRI `quad:"rdfs:subClassOf,optional"`
}

type PropertiesByIRI []Property

func (a PropertiesByIRI) Len() int      { return len(a) }
func (a PropertiesByIRI) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a PropertiesByIRI) Less(i, j int) bool {
	return a[i].ID < a[j].ID
}

type ClassesByIRI []Class

func (a ClassesByIRI) Len() int      { return len(a) }
func (a ClassesByIRI) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ClassesByIRI) Less(i, j int) bool {
	return a[i].ID < a[j].ID
}
