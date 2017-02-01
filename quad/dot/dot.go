// Package dot provides an encoder for DOT format (graphviz).
package dot

import (
	"fmt"
	"io"
	"strings"

	"github.com/cayleygraph/cayley/quad"
)

func init() {
	quad.RegisterFormat(quad.Format{
		Name:   "graphviz",
		Ext:    []string{".gv"},
		Writer: func(w io.Writer) quad.WriteCloser { return NewWriter(w) },
	})
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

type Writer struct {
	w       io.Writer
	written bool
	err     error
}

var escaper = strings.NewReplacer(
	`"`, `\"`,
)

func escape(s string) string {
	return `"` + escaper.Replace(s) + `"`
}

func (w *Writer) writeString(s string) {
	if w.err != nil {
		return
	}
	_, w.err = w.w.Write([]byte(s))
}

func (w *Writer) WriteQuad(q quad.Quad) error {
	if w.err != nil {
		return w.err
	}
	if !w.written {
		if _, err := w.w.Write([]byte(header)); err != nil {
			return err
		}
		w.written = true
	}
	// TODO: use label
	w.writeString("\t")
	w.writeString(escape(q.Subject.String()))
	w.writeString(" -> ")
	w.writeString(escape(q.Object.String()))
	w.writeString(" [ label = ")
	w.writeString(escape(q.Predicate.String()))
	w.writeString(" ];\n")
	return w.err
}

func (w *Writer) Close() error {
	if w.err != nil {
		return w.err
	}
	if !w.written {
		if _, w.err = w.w.Write([]byte(header)); w.err != nil {
			return w.err
		}
	}
	if _, w.err = w.w.Write([]byte(footer)); w.err != nil {
		return w.err
	}
	w.err = fmt.Errorf("closed")
	return nil
}

const header = `digraph cayley_graph {
`
const footer = "}\n"
