// Package dot is deprecated. Use github.com/cayleygraph/quad/dot.
package dot

import (
	"io"

	"github.com/cayleygraph/quad/dot"
)

func NewWriter(w io.Writer) *Writer {
	return dot.NewWriter(w)
}

type Writer = dot.Writer
