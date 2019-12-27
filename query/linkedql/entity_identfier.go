package linkedql

import "github.com/cayleygraph/quad"

// EntityIdentifier is an interface to be used where a single entity identifier is expected.
type EntityIdentifier interface {
	BuildIdentifier() (quad.Value, error)
}

// EntityIRI is an entity IRI.
type EntityIRI quad.IRI

// BuildIdentifier implements EntityIdentifier
func (i EntityIRI) BuildIdentifier() (quad.Value, error) {
	return quad.IRI(i), nil
}

// EntityBNode is an entity BNode.
type EntityBNode quad.BNode

// BuildIdentifier implements EntityIdentifier
func (i EntityBNode) BuildIdentifier() (quad.Value, error) {
	return quad.BNode(i), nil
}

// EntityIdentifierString is an entity IRI or BNode strings.
type EntityIdentifierString string

// BuildIdentifier implements EntityIdentifier
func (i EntityIdentifierString) BuildIdentifier() (quad.Value, error) {
	return parseIdentifier(string(i))
}
