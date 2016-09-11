// Package rdfs contains constants of the RDF Schema vocabulary (RDFS)
package rdfs

import "github.com/cayleygraph/cayley/voc"

func init() {
	voc.RegisterPrefix(Prefix, NS)
}

const (
	NS     = `http://www.w3.org/2000/01/rdf-schema#`
	Prefix = `rdfs:`
)

const (
	// Classes

	// The class resource, everything.
	Resource = Prefix + `Resource`
	// The class of classes.
	Class = Prefix + `Class`
	// The class of literal values, eg. textual strings and integers.
	Literal = Prefix + `Literal`
	// The class of RDF containers.
	Container = Prefix + `Container`
	// The class of RDF datatypes.
	Datatype = Prefix + `Datatype`
	// The class of container membership properties, rdf:_1, rdf:_2, ..., all of which are sub-properties of 'member'.
	ContainerMembershipProperty = Prefix + `ContainerMembershipProperty`

	// Properties

	// The subject is a subclass of a class.
	SubClassOf = Prefix + `subClassOf`
	// The subject is a subproperty of a property.
	SubPropertyOf = Prefix + `subPropertyOf`
	// A description of the subject resource.
	Comment = Prefix + `comment`
	// A human-readable name for the subject.
	Label = Prefix + `label`
	// A domain of the subject property.
	Domain = Prefix + `domain`
	// A range of the subject property.
	Range = Prefix + `range`
	// Further information about the subject resource.
	SeeAlso = Prefix + `seeAlso`
	// The defininition of the subject resource.
	IsDefinedBy = Prefix + `isDefinedBy`
	// A member of the subject resource.
	Member = Prefix + `member`
)
