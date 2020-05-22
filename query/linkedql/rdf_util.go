package linkedql

import (
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc/rdf"
	"github.com/cayleygraph/quad/voc/rdfs"
)

var (
	rdfType      = quad.IRI(rdf.Type).Full()
	rdfsResource = quad.IRI(rdfs.Resource).Full()
)

// MakeSingleEntityQuad creates a quad representing an entity only. The quad
// declares the entity is of type Resource, the base type of all entities in
// RDF.
func MakeSingleEntityQuad(id quad.Identifier) quad.Quad {
	return quad.Quad{Subject: id, Predicate: rdfType, Object: rdfsResource}
}

// IsSingleEntityQuad matches a quad representing only an entity.
func IsSingleEntityQuad(q quad.Quad) bool {
	// rdf:type rdfs:Resource is always true but not expressed in the graph.
	// it is used to specify an entity without specifying a property.
	return q.Predicate == rdfType && q.Object == rdfsResource
}
