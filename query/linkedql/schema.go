package linkedql

import (
	"reflect"

	"github.com/cayleygraph/quad"
)

var valueStep = reflect.TypeOf((*ValueStep)(nil)).Elem()
var step = reflect.TypeOf((*Step)(nil)).Elem()

func typeToRange(t reflect.Type) Identified {
	if t.Kind() == reflect.Slice {
		return typeToRange(t.Elem())
	}
	if t.Kind() == reflect.String {
		return Identified{ID: "xsd:string"}
	}
	if t.Kind() == reflect.Bool {
		return Identified{ID: "xsd:bool"}
	}
	if kind := t.Kind(); kind == reflect.Int64 || kind == reflect.Int {
		return Identified{ID: "xsd:int"}
	}
	if t.Implements(valueStep) {
		return Identified{ID: "linkedql:ValueStep"}
	}
	if t.Implements(step) {
		return Identified{ID: "linkedql:Step"}
	}
	if t.Implements(reflect.TypeOf((*Operator)(nil)).Elem()) {
		return Identified{ID: "linkedql:Operator"}
	}
	if t.Implements(reflect.TypeOf((*quad.Value)(nil)).Elem()) {
		return Identified{ID: "rdfs:Resource"}
	}
	panic("Unexpected type " + t.String())
}

type Identified struct {
	ID string `@id`
}

type CardinalityRestriction struct {
	ID          string     `json:"@id"`
	Type        string     `json:"@type"`
	Cardinality int        `json:"owl:cardinality"`
	Property    Identified `json:"owl:onProperty"`
}

type Property struct {
	ID     string     `json:"@id"`
	Type   string     `json:"@type"`
	Domain Identified `json:"rdfs:domain"`
	Range  Identified `json:"rdfs:range"`
}

type Class struct {
	ID         string       `json:"@id"`
	Type       string       `json:"@type"`
	SubClasses []Identified `json:"rdfs:subClassOf"`
}

func typeToDocuments(name string, t reflect.Type) []interface{} {
	var documents []interface{}
	var superClasses []Identified
	if t.Implements(valueStep) {
		superClasses = append(superClasses, Identified{ID: "linkedql:ValueStep"})
	} else {
		superClasses = append(superClasses, Identified{ID: "linkedql:Step"})
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		property := "linkedql:" + f.Tag.Get("json")
		if f.Type.Kind() != reflect.Slice {
			restriction := quad.RandomBlankNode().String()
			superClasses = append(superClasses, Identified{ID: restriction})
			documents = append(documents, CardinalityRestriction{
				ID:          restriction,
				Type:        "owl:Restriction",
				Cardinality: 1,
				Property:    Identified{ID: property},
			})
		}
		documents = append(documents, Property{
			ID:     property,
			Type:   "rdf:Property",
			Domain: Identified{ID: name},
			Range:  typeToRange(f.Type),
		})
	}
	documents = append(documents, Class{
		ID:         name,
		Type:       "rdfs:Class",
		SubClasses: superClasses,
	})
	return documents
}

// GenerateSchema for registered types. The schema is a collection of JSON-LD documents
// of the LinkedQL types and properties.
func GenerateSchema() []interface{} {
	var documents []interface{}
	for name, _type := range typeByName {
		for _, document := range typeToDocuments(name, _type) {
			documents = append(documents, document)
		}
	}
	return documents
}
