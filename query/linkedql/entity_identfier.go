package linkedql

import (
	"encoding/json"

	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc"
)

// EntityIdentifierI is an interface to be used where a single entity identifier is expected.
type EntityIdentifierI interface {
	BuildIdentifier(ns *voc.Namespaces) (quad.Value, error)
}

// EntityIdentifier is a struct wrapping the interface EntityIdentifierI
type EntityIdentifier struct {
	EntityIdentifierI
}

// NewEntityIdentifier constructs a new EntityIdentifer from a EntityIdentiferI
func NewEntityIdentifier(v EntityIdentifierI) EntityIdentifier {
	return EntityIdentifier{EntityIdentifierI: v}
}

// UnmarshalJSON implements RawMessage
func (p *EntityIdentifier) UnmarshalJSON(data []byte) error {
	var errors []error

	var iri EntityIRI
	err := json.Unmarshal(data, &iri)
	if err == nil {
		p.EntityIdentifierI = iri
		return nil
	}
	errors = append(errors, err)

	var bnode EntityBNode
	err = json.Unmarshal(data, &bnode)
	if err == nil {
		p.EntityIdentifierI = bnode
		return nil
	}
	errors = append(errors, err)

	var s EntityIdentifierString
	err = json.Unmarshal(data, &s)
	if err == nil {
		p.EntityIdentifierI = s
		return nil
	}
	errors = append(errors, err)

	return formatMultiError(errors)
}

// EntityIRI is an entity IRI.
type EntityIRI quad.IRI

// BuildIdentifier implements EntityIdentifier
func (iri EntityIRI) BuildIdentifier(ns *voc.Namespaces) (quad.Value, error) {
	return quad.IRI(iri).FullWith(ns), nil
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
