// Package pquads is deprecated. Use github.com/cayleygraph/quad/pquads.
package pquads

import (
	"io"

	"github.com/cayleygraph/quad/pquads"
)

var DefaultMaxSize = pquads.DefaultMaxSize

const ContentType = pquads.ContentType

type Writer = pquads.Writer

type Options = pquads.Options

// NewWriter creates protobuf quads encoder.
//
// Deprecated: use github.com/cayleygraph/quad/pquads package instead.
func NewWriter(w io.Writer, opts *Options) *Writer {
	return pquads.NewWriter(w, opts)
}

type Reader = pquads.Reader

// NewReader creates protobuf quads decoder.
//
// MaxSize argument limits maximal size of the buffer used to read quads.
//
// Deprecated: use github.com/cayleygraph/quad/pquads package instead.
func NewReader(r io.Reader, maxSize int) *Reader {
	return pquads.NewReader(r, maxSize)
}
