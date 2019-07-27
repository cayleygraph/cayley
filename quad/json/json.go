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
		MarshalValue: func(v quad.Value) ([]byte, error) {
			return json.Marshal(quad.ToString(v))
		},
		UnmarshalValue: func(b []byte) (quad.Value, error) {
			var s *string
			if err := json.Unmarshal(b, &s); err != nil {
				return nil, err
			} else if s == nil {
				return nil, nil
			}
			return quad.StringToValue(*s), nil
		},
	})
	quad.RegisterFormat(quad.Format{
		Name:   "json-stream",
		Mime:   []string{"application/x-json-stream"},
		Writer: func(w io.Writer) quad.WriteCloser { return NewStreamWriter(w) },
		Reader: func(r io.Reader) quad.ReadCloser { return NewStreamReader(r) },
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

func NewStreamReader(r io.Reader) *StreamReader {
	return &StreamReader{dec: json.NewDecoder(r)}
}

type StreamReader struct {
	dec *json.Decoder
	err error
}

func (r *StreamReader) ReadQuad() (quad.Quad, error) {
	if r.err != nil {
		return quad.Quad{}, r.err
	}
	var q quad.Quad
	r.err = r.dec.Decode(&q)
	return q, r.err
}
func (r *StreamReader) Close() error { return nil }

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

func (w *Writer) WriteQuads(buf []quad.Quad) (int, error) {
	for i, q := range buf {
		if err := w.WriteQuad(q); err != nil {
			return i, err
		}
	}
	return len(buf), nil
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

func NewStreamWriter(w io.Writer) *StreamWriter {
	return &StreamWriter{enc: json.NewEncoder(w)}
}

type StreamWriter struct {
	enc *json.Encoder
}

func (w *StreamWriter) WriteQuad(q quad.Quad) error {
	return w.enc.Encode(q)
}

func (w *StreamWriter) WriteQuads(buf []quad.Quad) (int, error) {
	for i, q := range buf {
		if err := w.WriteQuad(q); err != nil {
			return i, err
		}
	}
	return len(buf), nil
}

func (w *StreamWriter) Close() error { return nil }
