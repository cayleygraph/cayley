package linkedql

import (
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc"
)

// AbsoluteValue uses given ns to resolve short IRIs and types in typed strings to their full form
func AbsoluteValue(value quad.Value, ns *voc.Namespaces) quad.Value {
	switch v := value.(type) {
	case quad.IRI:
		return v.FullWith(ns)
	case quad.TypedString:
		return quad.TypedString{Value: v.Value, Type: v.Type.FullWith(ns)}
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
