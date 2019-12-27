package linkedql

import "github.com/cayleygraph/quad"

import "github.com/cayleygraph/quad/voc"

// EntityIdentifier is an interface to be used where a single entity identifier is expected.
type EntityIdentifier interface {
	BuildIdentifier(ns *voc.Namespaces) (quad.Value, error)
}

// EntityIRI is an entity IRI.
type EntityIRI quad.IRI

// BuildIdentifier implements EntityIdentifier
func (i EntityIRI) BuildIdentifier(ns *voc.Namespaces) (quad.Value, error) {
	return AbsoluteIRI(quad.IRI(i), ns), nil
}

// EntityBNode is an entity BNode.
type EntityBNode quad.BNode

// BuildIdentifier implements EntityIdentifier
func (i EntityBNode) BuildIdentifier(ns *voc.Namespaces) (quad.Value, error) {
	return quad.BNode(i), nil
}

// EntityIdentifierString is an entity IRI or BNode strings.
type EntityIdentifierString string

// BuildIdentifier implements EntityIdentifier
func (i EntityIdentifierString) BuildIdentifier(ns *voc.Namespaces) (quad.Value, error) {
	identifier, err := parseIdentifier(string(i))
	if err != nil {
		return nil, err
	}
	return AbsoluteValue(identifier, ns), nil
}
