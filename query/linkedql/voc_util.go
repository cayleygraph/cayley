package linkedql

import (
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc"
)

// AbsoluteIRI uses given ns to resolve short IRIs to their full form
func AbsoluteIRI(iri quad.IRI, ns *voc.Namespaces) quad.IRI {
	return quad.IRI(ns.FullIRI(string(iri)))
}

// AbsoluteValue uses given ns to resolve short IRIs and types in typed strings to their full form
func AbsoluteValue(value quad.Value, ns *voc.Namespaces) quad.Value {
	switch v := value.(type) {
	case quad.IRI:
		return AbsoluteIRI(v, ns)
	case quad.TypedString:
		return quad.TypedString{Value: v.Value, Type: AbsoluteIRI(v.Type, ns)}
	default:
		return v
	}
}

// AbsoluteValues applies AbsoluteValue on each item in provided values using provided ns
func AbsoluteValues(values []quad.Value, ns *voc.Namespaces) []quad.Value {
	var absoluteValues []quad.Value
	for _, value := range values {
		absoluteValues = append(absoluteValues, AbsoluteValue(value, ns))
	}
	return absoluteValues
}
