package linkedql

import (
	"fmt"
	"reflect"

	"github.com/cayleygraph/quad"
)

var valueStep = reflect.TypeOf((*ValueStep)(nil)).Elem()
var step = reflect.TypeOf((*Step)(nil)).Elem()

func typeToRange(t reflect.Type) string {
	if t.Kind() == reflect.Slice {
		return typeToRange(t.Elem())
	}
	if t.Kind() == reflect.String {
		return "xsd:string"
	}
	if t.Kind() == reflect.Bool {
		return "xsd:boolean"
	}
	if kind := t.Kind(); kind == reflect.Int64 || kind == reflect.Int {
		return "xsd:int"
	}
	if t.Implements(valueStep) {
		return "linkedql:ValueStep"
	}
	if t.Implements(step) {
		return "linkedql:Step"
	}
	if t.Implements(reflect.TypeOf((*Operator)(nil)).Elem()) {
		return "linkedql:Operator"
	}
	if t.Implements(reflect.TypeOf((*quad.Value)(nil)).Elem()) {
		return "rdfs:Resource"
	}
	panic("Unexpected type " + t.String())
}

// Identified is used for referencing a type
type Identified struct {
	ID string `json:"@id"`
}

// NewIdentified creates new identified struct
func NewIdentified(id string) Identified {
	return Identified{ID: id}
}

// CardinalityRestriction is used to indicate a how many values can a property get
type CardinalityRestriction struct {
	ID          string     `json:"@id"`
	Type        string     `json:"@type"`
	Cardinality int        `json:"owl:cardinality"`
	Property    Identified `json:"owl:onProperty"`
}

func NewBlankNodeID() string {
	return quad.RandomBlankNode().String()
}

// NewSingleCardinalityRestriction creates a cardinality of 1 restriction for given property
func NewSingleCardinalityRestriction(property string) CardinalityRestriction {
	return CardinalityRestriction{
		ID:          NewBlankNodeID(),
		Type:        "owl:Restriction",
		Cardinality: 1,
		Property:    Identified{ID: property},
	}
}

// GetOWLPropertyType for given kind of value type returns property OWL type
func GetOWLPropertyType(kind reflect.Kind) string {
	if kind == reflect.String || kind == reflect.Bool || kind == reflect.Int64 || kind == reflect.Int {
		return "owl:DatatypeProperty"
	}
	return "owl:ObjectProperty"
}

// Property is used to declare a property
type Property struct {
	ID     string      `json:"@id"`
	Type   string      `json:"@type"`
	Domain interface{} `json:"rdfs:domain"`
	Range  interface{} `json:"rdfs:range"`
}

// Class is used to declare a class
type Class struct {
	ID           string       `json:"@id"`
	Type         string       `json:"@type"`
	SuperClasses []Identified `json:"rdfs:subClassOf"`
}

// NewClass creates a new Class struct
func NewClass(id string, superClasses []Identified) Class {
	return Class{
		ID:           id,
		Type:         "rdfs:Class",
		SuperClasses: superClasses,
	}
}

// GetStepTypeClass for given step type returns the matching class identifier
func GetStepTypeClass(t reflect.Type) string {
	if t.Implements(valueStep) {
		return "linkedql:ValueStep"
	}
	return "linkedql:Step"
}

type List struct {
	ID      string        `json:"@id"`
	Members []interface{} `json:"@list"`
}

func NewList(members []interface{}) List {
	return List{
		ID:      NewBlankNodeID(),
		Members: members,
	}
}

type UnionOf struct {
	ID   string `json:"@id"`
	Type string `json:"@type"`
	List List   `json:"owl:unionOf"`
}

func NewUnionOf(classes []string) UnionOf {
	var members []interface{}
	for _, class := range classes {
		members = append(members, NewIdentified(class))
	}
	list := NewList(members)
	return UnionOf{
		ID:   NewBlankNodeID(),
		Type: "owl:Class",
		List: list,
	}
}

// GenerateSchema for registered types. The schema is a collection of JSON-LD documents
// of the LinkedQL types and properties.
func GenerateSchema() []interface{} {
	var documents []interface{}
	propertyToTypes := map[string]map[string]struct{}{}
	propertyToDomains := map[string]map[string]struct{}{}
	propertyToRanges := map[string]map[string]struct{}{}
	for name, t := range typeByName {
		superClasses := []Identified{
			NewIdentified(GetStepTypeClass(valueStep)),
		}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			property := "linkedql:" + f.Tag.Get("json")
			if f.Type.Kind() != reflect.Slice {
				restriction := NewSingleCardinalityRestriction(property)
				superClasses = append(superClasses, NewIdentified(restriction.ID))
				documents = append(documents, restriction)
			}
			_type := GetOWLPropertyType(f.Type.Kind())
			if propertyToTypes[property] == nil {
				propertyToTypes[property] = map[string]struct{}{}
			}
			propertyToTypes[property][_type] = struct{}{}
			if propertyToDomains[property] == nil {
				propertyToDomains[property] = map[string]struct{}{}
			}
			propertyToDomains[property][name] = struct{}{}
			if propertyToRanges[property] == nil {
				propertyToRanges[property] = map[string]struct{}{}
			}
			propertyToRanges[property][typeToRange(f.Type)] = struct{}{}
		}
		documents = append(documents, NewClass(name, superClasses))
	}
	for property, typeSet := range propertyToTypes {
		var types []string
		for _type := range typeSet {
			types = append(types, _type)
		}
		if len(types) != 1 {
			fmt.Printf("%v\n", propertyToRanges[property])
			panic("Properties must be either object properties or datatype properties. " + property + " has both.")
		}
		_type := types[0]
		var domains []string
		for domain := range propertyToDomains[property] {
			domains = append(domains, domain)
		}
		var ranges []string
		for _range := range propertyToRanges[property] {
			ranges = append(ranges, _range)
		}
		var domain interface{}
		var _range interface{}
		if len(domains) == 1 {
			domain = domains[0]
		} else {
			domain = NewUnionOf(domains)
		}
		if len(ranges) == 1 {
			_range = ranges[0]
		} else {
			_range = NewUnionOf(ranges)
		}
		documents = append(documents, Property{
			ID:     property,
			Type:   _type,
			Domain: domain,
			Range:  _range,
		})
	}
	return documents
}
