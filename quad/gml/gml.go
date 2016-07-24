// Package gml provides an encoder for Graph Modeling Format
package gml

import (
	"fmt"
	"io"
	"strings"

	"github.com/cayleygraph/cayley/quad"
)

func init() {
	quad.RegisterFormat(quad.Format{
		Name:   "gml",
		Ext:    []string{".gml"},
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

	nodes map[string]int
	cur   int
}

func (w *Writer) writeNode(s string) int {
	if w.err != nil {
		return -1
	}
	i, ok := w.nodes[s]
	if ok {
		return i
	}
	i = w.cur
	w.cur++
	w.nodes[s] = i
	_, w.err = fmt.Fprintf(w.w, "\tnode [ id %d label %s ]\n", i, escape(s))
	if w.err != nil {
		return -1
	}
	return i
}

var escaper = strings.NewReplacer( // TODO: ISO 8859-1?
	`&`, `&amp;`,
	`"`, `&quot;`,

//	`<`,`&lt;`,
//	`>`, `&gt;`,
)

func escape(s string) string {
	return `"` + escaper.Replace(s) + `"`
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
		w.nodes = make(map[string]int)
	}
	s := w.writeNode(q.Subject.String())
	o := w.writeNode(q.Object.String())
	if w.err != nil {
		return w.err
	}
	_, w.err = fmt.Fprintf(w.w, "\tedge [ source %d target %d label %s ]\n",
		s, o, escape(q.Predicate.String()))
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

const header = "Creator \"Cayley\"\ngraph [ directed 1\n"
const footer = "]\n"
