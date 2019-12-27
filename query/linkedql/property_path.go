package linkedql

import (
	"encoding/json"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
)

type propertyPathI interface {
	BuildPath(qs graph.QuadStore) (*path.Path, error)
}

// PropertyPath is an interface to be used where a path of properties is expected.
type PropertyPath struct {
	propertyPathI
}

// Type implements Step
func (*PropertyPath) Type() string {
	return Prefix + "PropertyPath"
}

// Description implements Step.
func (*PropertyPath) Description() string {
	return "PropertyPath is a string, multiple strins or path describing a set of properties"
}

// UnmarshalJSON implements RawMessage
func (p *PropertyPath) UnmarshalJSON(data []byte) (err error) {
	var errors []error

	var propertyIRIs PropertyIRIs
	err = json.Unmarshal(data, &propertyIRIs)
	if err == nil {
		p.propertyPathI = propertyIRIs
		return
	}
	errors = append(errors, err)

	var propertyIRIStrings PropertyIRIStrings
	err = json.Unmarshal(data, &propertyIRIStrings)
	if err == nil {
		p.propertyPathI = propertyIRIStrings
		return
	}
	errors = append(errors, err)

	var propertyIRI PropertyIRI
	err = json.Unmarshal(data, &propertyIRI)
	if err == nil {
		p.propertyPathI = propertyIRI
		return
	}
	errors = append(errors, err)

	var propertyIRIString PropertyIRIString
	err = json.Unmarshal(data, &propertyIRIString)
	if err == nil {
		p.propertyPathI = propertyIRIString
		return
	}
	errors = append(errors, err)

	return formatMultiError(errors)
}

// PropertyIRIs is a slice of property IRIs.
type PropertyIRIs []quad.IRI

// BuildPath implements PropertyPath.
func (p PropertyIRIs) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	var values []quad.Value
	for _, iri := range p {
		values = append(values, iri)
	}
	vertex := &Vertex{Values: values}
	return vertex.BuildPath(qs)
}

// PropertyIRIStrings is a slice of property IRI strings.
type PropertyIRIStrings []string

// BuildPath implements PropertyPath.
func (p PropertyIRIStrings) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	var iris PropertyIRIs
	for _, iri := range p {
		iris = append(iris, quad.IRI(iri))
	}
	return iris.BuildPath(qs)
}

// PropertyIRI is an IRI of a Property
type PropertyIRI quad.IRI

// BuildPath implements PropertyPath
func (p PropertyIRI) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	vertex := &Vertex{Values: []quad.Value{quad.IRI(p)}}
	return vertex.BuildPath(qs)
}

// PropertyIRIString is a string of IRI of a Property
type PropertyIRIString string

// BuildPath implements PropertyPath
func (p PropertyIRIString) BuildPath(qs graph.QuadStore) (*path.Path, error) {
	iri := PropertyIRI(p)
	return iri.BuildPath(qs)
}
