package quad

import (
	"github.com/cayleygraph/quad"
)

func IsValidValue(v Value) bool {
	return quad.IsValidValue(v)
}

// Value is a type used by all quad directions.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
type Value = quad.Value

type TypedStringer interface {
	TypedString() TypedString
}

// Equaler interface is implemented by values, that needs a special equality check.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
type Equaler = quad.Value

// HashSize is a size of the slice, returned by HashOf.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
const HashSize = quad.HashSize

// HashOf calculates a hash of value v.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
func HashOf(v Value) []byte {
	return quad.HashOf(v)
}

// HashTo calculates a hash of value v, storing it in a slice p.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
func HashTo(v Value, p []byte) {
	quad.HashTo(v, p)
}

// StringOf safely call v.String, returning empty string in case of nil Value.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
func StringOf(v Value) string {
	return quad.StringOf(v)
}

// NativeOf safely call v.Native, returning nil in case of nil Value.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
func NativeOf(v Value) interface{} {
	return quad.NativeOf(v)
}

// AsValue converts native type into closest Value representation.
// It returns false if type was not recognized.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
func AsValue(v interface{}) (Value, bool) {
	return quad.AsValue(v)
}

// StringToValue is a function to convert strings to typed
// quad values.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
func StringToValue(v string) Value {
	return quad.StringToValue(v)
}

// ToString casts a values to String or falls back to StringOf.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
func ToString(v Value) string {
	return quad.ToString(v)
}

// Raw is a Turtle/NQuads-encoded value.
//
// Deprecated: use IRI or String instead.
func Raw(s string) Value {
	return quad.Raw(s)
}

// String is an RDF string value (ex: "name").
//
// Deprecated: use github.com/cayleygraph/quad package instead.
type String = quad.String

// TypedString is an RDF value with type (ex: "name"^^<type>).
//
// Deprecated: use github.com/cayleygraph/quad package instead.
type TypedString = quad.TypedString

// LangString is an RDF string with language (ex: "name"@lang).
//
// Deprecated: use github.com/cayleygraph/quad package instead.
type LangString = quad.LangString

// IRIFormat is a format of IRI.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
type IRIFormat = quad.IRIFormat

const (
	// IRIDefault preserves current IRI formatting.
	IRIDefault = quad.IRIDefault
	// IRIShort changes IRI to use a short namespace prefix (ex: <rdf:type>).
	IRIShort = quad.IRIShort
	// IRIFull changes IRI to use full form (ex: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>).
	IRIFull = quad.IRIFull
)

// IRI is an RDF Internationalized Resource Identifier (ex: <name>).
//
// Deprecated: use github.com/cayleygraph/quad package instead.
type IRI = quad.IRI

// BNode is an RDF Blank Node (ex: _:name).
//
// Deprecated: use github.com/cayleygraph/quad package instead.
type BNode = quad.BNode

// StringConversion is a function to convert string values with a
// specific IRI type to their native equivalents.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
type StringConversion = quad.StringConversion

// RegisterStringConversion will register an automatic conversion of
// TypedString values with provided type to a native equivalent such as Int, Time, etc.
//
// If fnc is nil, automatic conversion from selected type will be removed.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
func RegisterStringConversion(dataType IRI, fnc StringConversion) {
	quad.RegisterStringConversion(dataType, fnc)
}

// Int is a native wrapper for int64 type.
//
// It uses NQuad notation similar to TypedString.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
type Int = quad.Int

// Float is a native wrapper for float64 type.
//
// It uses NQuad notation similar to TypedString.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
type Float = quad.Float

// Bool is a native wrapper for bool type.
//
// It uses NQuad notation similar to TypedString.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
type Bool = quad.Bool

// Time is a native wrapper for time.Time type.
//
// It uses NQuad notation similar to TypedString.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
type Time = quad.Time

type ByValueString = quad.ByValueString

// Sequence is an object to generate a sequence of Blank Nodes.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
type Sequence = quad.Sequence

// RandomBlankNode returns a randomly generated Blank Node.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
func RandomBlankNode() BNode {
	return quad.RandomBlankNode()
}
