// Package pquads implements Cayley-specific protobuf-based quads format.
package pquads

import (
	"fmt"
	"github.com/cayleygraph/cayley/quad"
	pio "github.com/gogo/protobuf/io"
	"io"
)

var DefaultMaxSize = 1024 * 1024

func init() {
	quad.RegisterFormat(quad.Format{
		Name:   "pquads",
		Ext:    []string{".pq"},
		Mime:   []string{"application/x-protobuf", "application/octet-stream"},
		Writer: func(w io.Writer) quad.WriteCloser { return NewWriter(w, true) },
		Reader: func(r io.Reader) quad.ReadCloser { return NewReader(r, DefaultMaxSize) },
	})
}

type Writer struct {
	pw      pio.WriteCloser
	err     error
	comp    bool
	s, p, o quad.Value
}

// NewWriter creates protobuf quads encoder.
func NewWriter(w io.Writer, compact bool) *Writer {
	pw := pio.NewDelimitedWriter(w)
	err := pw.WriteMsg(&Header{Version: 1, Full: !compact})
	return &Writer{pw: pw, err: err, comp: compact}
}
func (w *Writer) WriteQuad(q quad.Quad) error {
	if w.err != nil {
		return w.err
	}
	if w.comp {
		if q.Subject == w.s {
			q.Subject = nil
		} else {
			w.s = q.Subject
		}
		if q.Predicate == w.p {
			q.Predicate = nil
		} else {
			w.p = q.Predicate
		}
		if q.Object == w.o {
			q.Object = nil
		} else {
			w.o = q.Object
		}
	}
	w.err = w.pw.WriteMsg(MakeQuad(q))
	return w.err
}
func (w *Writer) Close() error {
	return w.pw.Close()
}

type Reader struct {
	pr      pio.ReadCloser
	err     error
	comp    bool
	s, p, o quad.Value
}

// NewReader creates protobuf quads decoder.
//
// MaxSize argument limits maximal size of the buffer used to read quads.
func NewReader(r io.Reader, maxSize int) *Reader {
	if maxSize <= 0 {
		maxSize = DefaultMaxSize
	}
	pr := pio.NewDelimitedReader(r, maxSize)
	var h Header
	qr := &Reader{pr: pr}
	if err := pr.ReadMsg(&h); err != nil {
		qr.err = err
	} else if h.Version != 1 {
		qr.err = fmt.Errorf("unsupported pquads version: %d", h.Version)
	}
	qr.comp = !h.Full
	return qr
}
func (r *Reader) ReadQuad() (quad.Quad, error) {
	if r.err != nil {
		return quad.Quad{}, r.err
	}
	var pq Quad
	if r.err = r.pr.ReadMsg(&pq); r.err != nil {
		return quad.Quad{}, r.err
	}
	q := pq.ToNative()
	if q.Subject == nil {
		q.Subject = r.s
	} else {
		r.s = q.Subject
	}
	if q.Predicate == nil {
		q.Predicate = r.p
	} else {
		r.p = q.Predicate
	}
	if q.Object == nil {
		q.Object = r.o
	} else {
		r.o = q.Object
	}
	return q, nil
}
func (r *Reader) Close() error {
	return r.pr.Close()
}
