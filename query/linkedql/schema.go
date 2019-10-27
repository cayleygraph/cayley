package linkedql

import (
	"reflect"

	"github.com/cayleygraph/quad"
)

var valueStep = reflect.TypeOf((*ValueStep)(nil)).Elem()
var step = reflect.TypeOf((*Step)(nil)).Elem()

func typeToRange(t reflect.Type) interface{} {
	if t.Kind() == reflect.Slice {
		return typeToRange(t.Elem())
	}
	if t.Kind() == reflect.String {
		return map[string]interface{}{"@id": "xsd:string"}
	}
	if t.Kind() == reflect.Bool {
		return map[string]interface{}{"@id": "xsd:bool"}
	}
	if kind := t.Kind(); kind == reflect.Int64 || kind == reflect.Int {
		return map[string]interface{}{"@id": "xsd:int"}
	}
	if t.Implements(valueStep) {
		return map[string]interface{}{"@id": "linkedql:ValueStep"}
	}
	if t.Implements(step) {
		return map[string]interface{}{"@id": "linkedql:Step"}
	}
	if t.Implements(reflect.TypeOf((*Operator)(nil)).Elem()) {
		return map[string]interface{}{"@id": "linkedql:Operator"}
	}
	if t.Implements(reflect.TypeOf((*quad.Value)(nil)).Elem()) {
		return map[string]interface{}{"@id": "rdfs:Resource"}
	}
	panic("Unexpected type " + t.String())
}

func typeToDocuments(name string, t reflect.Type) []interface{} {
	var documents []interface{}
	var superClasses []string
	if t.Implements(valueStep) {
		superClasses = append(superClasses, "linkedql:ValueStep")
	} else {
		superClasses = append(superClasses, "linkedql:Step")
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		property := "linkedql:" + f.Tag.Get("json")
		if f.Type.Kind() != reflect.Slice {
			restriction := quad.RandomBlankNode().String()
			superClasses = append(superClasses, restriction)
			documents = append(documents, map[string]interface{}{
				"@id":             restriction,
				"@type":           "owl:Restriction",
				"owl:cardinality": 1,
				"owl:onProperty":  map[string]interface{}{"@id": property},
			})
		}
		documents = append(documents, map[string]interface{}{
			// TODO(iddan): use json tag instead
			"@id":         property,
			"@type":       "rdf:Property",
			"rdfs:domain": map[string]interface{}{"@id": name},
			"rdfs:range":  typeToRange(f.Type),
		})
	}
	documents = append(documents, map[string]interface{}{
		"@id":             name,
		"@type":           "rdfs:Class",
		"rdfs:subClassOf": superClasses,
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
