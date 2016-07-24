// Package json provides an encoder/decoder for JSON quad formats
package json

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/cayleygraph/cayley/quad"
)

func init() {
	quad.RegisterFormat(quad.Format{
		Name:   "json",
		Ext:    []string{".json"},
		Mime:   []string{"application/json"},
		Writer: func(w io.Writer) quad.WriteCloser { return NewWriter(w) },
		Reader: func(r io.Reader) quad.ReadCloser { return NewReader(r) },
	})
}

func NewReader(r io.Reader) *Reader {
	var quads []quad.Quad
	err := json.NewDecoder(r).Decode(&quads)
	return &Reader{ // TODO(dennwc): stream-friendly reader
		quads: quads,
		err:   err,
	}
}

type Reader struct {
	quads []quad.Quad
	n     int
	err   error
}

func (r *Reader) ReadQuad() (quad.Quad, error) {
	if r.err != nil {
		return quad.Quad{}, r.err
	}
	if r.n >= len(r.quads) {
		return quad.Quad{}, io.EOF
	}
	q := r.quads[r.n]
	r.n++
	if !q.IsValid() {
		return quad.Quad{}, fmt.Errorf("invalid quad at index %d. %s", r.n-1, q)
	}
	return q, nil
}
func (r *Reader) Close() error { return nil }

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

type Writer struct {
	w       io.Writer
	written bool
	closed  bool
}

func (w *Writer) WriteQuad(q quad.Quad) error {
	if w.closed {
		return fmt.Errorf("closed")
	}
	if !w.written {
		if _, err := w.w.Write([]byte("[\n\t")); err != nil {
			return err
		}
		w.written = true
	} else {
		if _, err := w.w.Write([]byte(",\n\t")); err != nil {
			return err
		}
	}
	data, err := json.Marshal(q)
	if err != nil {
		return err
	}
	_, err = w.w.Write(data)
	return err
}

func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if !w.written {
		_, err := w.w.Write([]byte("null\n"))
		return err
	}
	_, err := w.w.Write([]byte("\n]\n"))
	return err
}
