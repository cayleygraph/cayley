// Package rdfs is deprecated. Use github.com/cayleygraph/quad/voc/rdfs.
package rdfs

import "github.com/cayleygraph/quad/voc/rdfs"

const (
	NS     = rdfs.NS
	Prefix = rdfs.Prefix
)

const (
	// Classes

	// The class resource, everything.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdfs package instead.
	Resource = rdfs.Resource
	// The class of classes.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdfs package instead.
	Class = rdfs.Class
	// The class of literal values, eg. textual strings and integers.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdfs package instead.
	Literal = rdfs.Literal
	// The class of RDF containers.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdfs package instead.
	Container = rdfs.Container
	// The class of RDF datatypes.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdfs package instead.
	Datatype = rdfs.Datatype
	// The class of container membership properties, rdf:_1, rdf:_2, ..., all of which are sub-properties of 'member'.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdfs package instead.
	ContainerMembershipProperty = rdfs.ContainerMembershipProperty

	// Properties

	// The subject is a subclass of a class.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdfs package instead.
	SubClassOf = rdfs.SubClassOf
	// The subject is a subproperty of a property.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdfs package instead.
	SubPropertyOf = rdfs.SubPropertyOf
	// A description of the subject resource.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdfs package instead.
	Comment = rdfs.Comment
	// A human-readable name for the subject.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdfs package instead.
	Label = rdfs.Label
	// A domain of the subject property.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdfs package instead.
	Domain = rdfs.Domain
	// A range of the subject property.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdfs package instead.
	Range = rdfs.Range
	// Further information about the subject resource.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdfs package instead.
	SeeAlso = rdfs.SeeAlso
	// The defininition of the subject resource.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdfs package instead.
	IsDefinedBy = rdfs.IsDefinedBy
	// A member of the subject resource.
	//
	// Deprecated: use github.com/cayleygraph/quad/voc/rdfs package instead.
	Member = rdfs.Member
)
