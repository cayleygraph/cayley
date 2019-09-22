package pquads

import (
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/pquads"
)

// MakeValue converts quad.Value to its protobuf representation.
//
// Deprecated: use github.com/cayleygraph/quad/pquads package instead.
func MakeValue(qv quad.Value) *Value {
	return pquads.MakeValue(qv)
}

// MarshalValue is a helper for serialization of quad.Value.
//
// Deprecated: use github.com/cayleygraph/quad/pquads package instead.
func MarshalValue(v quad.Value) ([]byte, error) {
	return pquads.MarshalValue(v)
}

// UnmarshalValue is a helper for deserialization of quad.Value.
//
// Deprecated: use github.com/cayleygraph/quad/pquads package instead.
func UnmarshalValue(data []byte) (quad.Value, error) {
	return pquads.UnmarshalValue(data)
}

// MakeQuad converts quad.Quad to its protobuf representation.
//
// Deprecated: use github.com/cayleygraph/quad/pquads package instead.
func MakeQuad(q quad.Quad) *Quad {
	return pquads.MakeQuad(q)
}
