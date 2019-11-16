package schema

import (
	"encoding/json"
	"reflect"

	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc/rdfs"
)

var (
	pathStep = reflect.TypeOf((*linkedql.PathStep)(nil)).Elem()
	value    = reflect.TypeOf((*quad.Value)(nil)).Elem()
	operator = reflect.TypeOf((*linkedql.Operator)(nil)).Elem()
)

func typeToRange(t reflect.Type) string {
	if t.Kind() == reflect.Slice {
		return typeToRange(t.Elem())
	}
	// TODO: add XSD types to voc package
	if t.Kind() == reflect.String {
		return "xsd:string"
	}
	if t.Kind() == reflect.Bool {
		return "xsd:boolean"
	}
	if kind := t.Kind(); kind == reflect.Int64 || kind == reflect.Int {
		return "xsd:int"
	}
	if t.Implements(pathStep) {
		return linkedql.Prefix + "PathStep"
	}
	if t.Implements(operator) {
		return linkedql.Prefix + "Operator"
	}
	if t.Implements(value) {
		return rdfs.Resource
	}
	panic("Unexpected type " + t.String())
}

// identified is used for referencing a type
type identified struct {
	ID string `json:"@id"`
}

// newIdentified creates new identified struct
func newIdentified(id string) identified {
	return identified{ID: id}
}

// cardinalityRestriction is used to indicate a how many values can a property get
type cardinalityRestriction struct {
	ID          string     `json:"@id"`
	Type        string     `json:"@type"`
	Cardinality int        `json:"owl:cardinality"`
	Property    identified `json:"owl:onProperty"`
}

func newBlankNodeID() string {
	return quad.RandomBlankNode().String()
}

// newSingleCardinalityRestriction creates a cardinality of 1 restriction for given property
func newSingleCardinalityRestriction(prop string) cardinalityRestriction {
	return cardinalityRestriction{
		ID:          newBlankNodeID(),
		Type:        "owl:Restriction",
		Cardinality: 1,
		Property:    identified{ID: prop},
	}
}

// getOWLPropertyType for given kind of value type returns property OWL type
func getOWLPropertyType(kind reflect.Kind) string {
	if kind == reflect.String || kind == reflect.Bool || kind == reflect.Int64 || kind == reflect.Int {
		return "owl:DatatypeProperty"
	}
	return "owl:ObjectProperty"
}

// property is used to declare a property
type property struct {
	ID     string      `json:"@id"`
	Type   string      `json:"@type"`
	Domain interface{} `json:"rdfs:domain"`
	Range  interface{} `json:"rdfs:range"`
}

// class is used to declare a class
type class struct {
	ID           string        `json:"@id"`
	Type         string        `json:"@type"`
	Comment      string        `json:"rdfs:comment"`
	SuperClasses []interface{} `json:"rdfs:subClassOf"`
}

// newClass creates a new class struct
func newClass(id string, superClasses []interface{}, comment string) class {
	return class{
		ID:           id,
		Type:         rdfs.Class,
		SuperClasses: superClasses,
		Comment:      comment,
	}
}

// getStepTypeClass for given step type returns the matching class identifier
func getStepTypeClass(t reflect.Type) string {
	if t.Implements(pathStep) {
		return linkedql.Prefix + "PathStep"
	}
	return linkedql.Prefix + "Step"
}

type list struct {
	ID      string        `json:"@id"`
	Members []interface{} `json:"@list"`
}

func newList(members []interface{}) list {
	return list{
		ID:      newBlankNodeID(),
		Members: members,
	}
}

type unionOf struct {
	ID   string `json:"@id"`
	Type string `json:"@type"`
	List list   `json:"owl:unionOf"`
}

func newUnionOf(classes []string) unionOf {
	var members []interface{}
	for _, class := range classes {
		members = append(members, newIdentified(class))
	}
	return unionOf{
		ID:   newBlankNodeID(),
		Type: "owl:Class",
		List: newList(members),
	}
}

// Generate a schema in JSON-LD format that contains all registered LinkedQL types and properties.
func Generate() []byte {
	var out []interface{}
	propToTypes := make(map[string]map[string]struct{})
	propToDomains := make(map[string]map[string]struct{})
	propToRanges := make(map[string]map[string]struct{})
	for name, t := range linkedql.TypeByName {
		step, ok := reflect.New(t).Interface().(linkedql.Step)
		if !ok {
			continue
		}
		super := []interface{}{
			newIdentified(getStepTypeClass(pathStep)),
		}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if f.Anonymous {
				t = f.Type
				if t.Kind() == reflect.Struct {
					for j := 0; j < t.NumField(); j++ {
						f = t.Field(j)
						prop := linkedql.Prefix + f.Tag.Get("json")
						if f.Type.Kind() != reflect.Slice {
							super = append(super, newSingleCardinalityRestriction(prop))
						}
						typ := getOWLPropertyType(f.Type.Kind())

						if propToTypes[prop] == nil {
							propToTypes[prop] = make(map[string]struct{})
						}
						propToTypes[prop][typ] = struct{}{}

						if propToDomains[prop] == nil {
							propToDomains[prop] = make(map[string]struct{})
						}
						propToDomains[prop][name] = struct{}{}

						if propToRanges[prop] == nil {
							propToRanges[prop] = make(map[string]struct{})
						}
						propToRanges[prop][typeToRange(f.Type)] = struct{}{}
					}
					continue
				}
			}
			prop := linkedql.Prefix + f.Tag.Get("json")
			if f.Type.Kind() != reflect.Slice {
				super = append(super, newSingleCardinalityRestriction(prop))
			}
			typ := getOWLPropertyType(f.Type.Kind())

			if propToTypes[prop] == nil {
				propToTypes[prop] = make(map[string]struct{})
			}
			propToTypes[prop][typ] = struct{}{}

			if propToDomains[prop] == nil {
				propToDomains[prop] = make(map[string]struct{})
			}
			propToDomains[prop][name] = struct{}{}

			if propToRanges[prop] == nil {
				propToRanges[prop] = make(map[string]struct{})
			}
			propToRanges[prop][typeToRange(f.Type)] = struct{}{}
		}
		out = append(out, newClass(name, super, step.Description()))
	}
	for prop, types := range propToTypes {
		if len(types) != 1 {
			panic("Properties must be either object properties or datatype properties. " + prop + " has both.")
		}
		var typ string
		for t := range types {
			typ = t
			break
		}
		var domains []string
		for d := range propToDomains[prop] {
			domains = append(domains, d)
		}
		var ranges []string
		for r := range propToRanges[prop] {
			ranges = append(ranges, r)
		}
		var dom interface{}
		if len(domains) == 1 {
			dom = domains[0]
		} else {
			dom = newUnionOf(domains)
		}
		var rng interface{}
		if len(ranges) == 1 {
			rng = ranges[0]
		} else {
			rng = newUnionOf(ranges)
		}
		out = append(out, property{
			ID:     prop,
			Type:   typ,
			Domain: dom,
			Range:  rng,
		})
	}
	data, err := json.Marshal(out)
	if err != nil {
		panic(err)
	}
	return data
}
