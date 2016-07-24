// Package graphml provides an encoder for GraphML format
package graphml

import (
	"encoding/xml"
	"fmt"
	"io"

	"github.com/cayleygraph/cayley/quad"
)

func init() {
	quad.RegisterFormat(quad.Format{
		Name:   "graphml",
		Ext:    []string{".graphml"},
		Mime:   []string{"application/xml"},
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
	_, w.err = fmt.Fprintf(w.w, "\t\t<node id=\"n%d\"><data key=\"d0\">", i)
	if w.err != nil {
		return -1
	}
	if w.err = xml.EscapeText(w.w, []byte(s)); w.err != nil {
		return -1
	}
	if _, w.err = w.w.Write([]byte("</data></node>\n")); w.err != nil {
		return -1
	}
	return i
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
	_, w.err = fmt.Fprintf(w.w, "\t\t<edge source=\"n%d\" target=\"n%d\"><data key=\"d1\">", s, o)
	if w.err != nil {
		return w.err
	}
	if w.err = xml.EscapeText(w.w, []byte(q.Predicate.String())); w.err != nil {
		return w.err
	}
	_, w.err = w.w.Write([]byte("</data></edge>\n"))
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

const header = `<?xml version="1.0" encoding="UTF-8"?>
<graphml xmlns="http://graphml.graphdrawing.org/xmlns"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">
	<key id="d0" for="node" attr.name="description" attr.type="string"/>
	<key id="d1" for="edge" attr.name="description" attr.type="string"/>
	<graph id="G" edgedefault="directed">
`
const footer = "\t</graph>\n</graphml>\n"
