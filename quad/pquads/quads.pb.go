package pquads

import (
	"github.com/cayleygraph/quad/pquads"
)
import _ "github.com/gogo/protobuf/gogoproto"

// Quad is in internal representation of quad used by Cayley.
//
// Deprecated: use github.com/cayleygraph/quad/pquads package instead.
type Quad = pquads.Quad

// WireQuad is a quad that allows any value for it's directions.
//
// Deprecated: use github.com/cayleygraph/quad/pquads package instead.
type WireQuad = pquads.WireQuad

// StrictQuad is a quad as described by RDF spec.
//
// Deprecated: use github.com/cayleygraph/quad/pquads package instead.
type StrictQuad = pquads.StrictQuad

type StrictQuad_Ref = pquads.StrictQuad_Ref

type StrictQuad_Ref_BnodeLabel = pquads.StrictQuad_Ref_BnodeLabel
type StrictQuad_Ref_Iri = pquads.StrictQuad_Ref_Iri

type Value = pquads.Value

type Value_Raw = pquads.Value_Raw
type Value_Str = pquads.Value_Str
type Value_Iri = pquads.Value_Iri
type Value_Bnode = pquads.Value_Bnode
type Value_TypedStr = pquads.Value_TypedStr
type Value_LangStr = pquads.Value_LangStr
type Value_Int = pquads.Value_Int
type Value_Float = pquads.Value_Float
type Value_Boolean = pquads.Value_Boolean
type Value_Time = pquads.Value_Time

type Value_TypedString = pquads.Value_TypedString
type Value_LangString = pquads.Value_LangString
type Value_Timestamp = pquads.Value_Timestamp

type Header = pquads.Header

var (
	ErrInvalidLengthQuads = pquads.ErrInvalidLengthQuads
	ErrIntOverflowQuads   = pquads.ErrIntOverflowQuads
)
