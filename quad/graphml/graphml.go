// Package graphml is deprecated. Use github.com/cayleygraph/quad/graphml.
package graphml

import (
	"io"

	"github.com/cayleygraph/quad/graphml"
)

func NewWriter(w io.Writer) *Writer {
	return graphml.NewWriter(w)
}

type Writer = graphml.Writer
