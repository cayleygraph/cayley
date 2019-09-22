// Package gml is deprecated. Use github.com/cayleygraph/quad/gml.
package gml

import (
	"io"

	"github.com/cayleygraph/quad/gml"
)

func NewWriter(w io.Writer) *Writer {
	return gml.NewWriter(w)
}

type Writer = gml.Writer
