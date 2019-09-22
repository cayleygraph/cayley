// Package jsonld is deprecated. Use github.com/cayleygraph/quad/jsonld.
package jsonld

import (
	"io"

	"github.com/cayleygraph/quad/jsonld"
)

// AutoConvertTypedString allows to convert TypedString values to native
// equivalents directly while parsing. It will call ToNative on all TypedString values.
//
// If conversion error occurs, it will preserve original TypedString value.
//
// Deprecated: use github.com/cayleygraph/quad/jsonld package instead.
var AutoConvertTypedString = jsonld.AutoConvertTypedString

// NewReader returns quad reader for JSON-LD stream.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
func NewReader(r io.Reader) *Reader {
	return jsonld.NewReader(r)
}

// NewReaderFromMap returns quad reader for JSON-LD map object.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
func NewReaderFromMap(o interface{}) *Reader {
	return jsonld.NewReaderFromMap(o)
}

type Reader = jsonld.Reader

func NewWriter(w io.Writer) *Writer {
	return jsonld.NewWriter(w)
}

type Writer = jsonld.Writer
