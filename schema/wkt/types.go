package wkt

import (
	"github.com/cayleygraph/cayley/quad"
)

type Object struct {
	ID      quad.IRI `quad:"@id"`
	Type    string   `quad:"@type"`
	Name    string   `quad:"rdfs:label,optional"`
	Comment string   `quad:"rdfs:comment,optional"`
}

type Property struct {
	ID      quad.IRI   `quad:"@id"`
	rdfType struct{}   `quad:"@type > rdf:Property"`
	Name    string     `quad:"rdfs:label,optional"`
	Comment string     `quad:"rdfs:comment,optional"`
	Expects []quad.IRI `quad:"schema:rangeIncludes,optional"`

	InverseOf    quad.IRI   `quad:"schema:inverseOf,optional"`
	SupersededBy []quad.IRI `quad:"schema:supersededBy,optional"`
}

type Class struct {
	ID         quad.IRI   `quad:"@id"`
	rdfType    struct{}   `quad:"@type > rdfs:Class"`
	Name       string     `quad:"rdfs:label,optional"`
	Comment    string     `quad:"rdfs:comment,optional"`
	Properties []Property `quad:"schema:domainIncludes < *,optional"`
	Extends    []quad.IRI `quad:"rdfs:subClassOf,optional"`
	Specific   []quad.IRI `quad:"rdfs:subClassOf < *,optional"`
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
