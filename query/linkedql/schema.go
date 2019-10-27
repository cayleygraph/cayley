package linkedql

import (
	"encoding/json"
	"fmt"
	"reflect"
	"unicode"
	"unicode/utf8"

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
	fmt.Printf("%v", t)
	panic("Unexpected type")
}

func lcFirst(str string) string {
	rune, size := utf8.DecodeRuneInString(str)
	return string(unicode.ToLower(rune)) + str[size:]
}

func typeToDocuments(name string, t reflect.Type) []interface{} {
	var documents []interface{}
	var superClass string
	if t.Implements(valueStep) {
		superClass = "linkedql:ValueStep"
	} else {
		superClass = "linkedql:Step"
	}
	documents = append(documents, map[string]interface{}{
		"@id":             name,
		"@type":           "rdfs:Class",
		"rdfs:subClassOf": superClass,
	})
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		documents = append(documents, map[string]interface{}{
			"@id":         "linkedql:" + lcFirst(f.Name),
			"@type":       "rdf:Property",
			"rdfs:domain": map[string]interface{}{"@id": name},
			"rdfs:range":  typeToRange(f.Type),
		})
	}
	return documents
}

func generateSchema() []interface{} {
	var documents []interface{}
	for name, _type := range typeByName {
		for _, document := range typeToDocuments(name, _type) {
			documents = append(documents, document)
		}
	}
	return documents
}

func serializeSchema() []byte {
	bytes, err := json.MarshalIndent(generateSchema(), "", "    ")
	if err != nil {
		panic(err)
	}
	return bytes
}
