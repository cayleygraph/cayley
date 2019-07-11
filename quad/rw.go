package quad

import (
	"io"
)

// Writer is a minimal interface for quad writers. Used for quad serializers and quad stores.
type Writer interface {
	// WriteQuad writes a single quad and returns an error, if any.
	//
	// Deprecated: use WriteQuads instead.
	WriteQuad(Quad) error
	BatchWriter
}

type WriteCloser interface {
	Writer
	io.Closer
}

// BatchWriter is an interface for writing quads in batches.
type BatchWriter interface {
	// WriteQuads returns a number of quads that where written and an error, if any.
	WriteQuads(buf []Quad) (int, error)
}

// Reader is a minimal interface for quad readers. Used for quad deserializers and quad iterators.
//
// ReadQuad reads next valid Quad. It returns io.EOF if no quads are left.
type Reader interface {
	ReadQuad() (Quad, error)
}

// Skipper is an interface for quad reader that can skip quads efficiently without decoding them.
//
// It returns io.EOF if no quads are left.
type Skipper interface {
	SkipQuad() error
}

type ReadCloser interface {
	Reader
	io.Closer
}

type ReadSkipCloser interface {
	Reader
	Skipper
	io.Closer
}

// BatchReader is an interface for reading quads in batches.
//
// ReadQuads reads at most len(buf) quads into buf. It returns number of quads that were read and an error.
// It returns an io.EOF if there is no more quads to read.
type BatchReader interface {
	ReadQuads(buf []Quad) (int, error)
}

type Quads struct {
	s []Quad
}

func (r *Quads) WriteQuad(q Quad) error {
	r.s = append(r.s, q)
	return nil
}

func (r *Quads) ReadQuad() (Quad, error) {
	if r == nil || len(r.s) == 0 {
		return Quad{}, io.EOF
	}
	q := r.s[0]
	r.s = r.s[1:]
	if len(r.s) == 0 {
		r.s = nil
	}
	return q, nil
}

// NewReader creates a quad reader from a quad slice.
func NewReader(quads []Quad) *Quads {
	return &Quads{s: quads}
}

// Copy will copy all quads from src to dst. It returns copied quads count and an error, if it failed.
//
// Copy will try to cast dst to BatchWriter and will switch to CopyBatch implementation in case of success.
func Copy(dst Writer, src Reader) (n int, err error) {
	if bw, ok := dst.(BatchWriter); ok {
		return CopyBatch(bw, src, 0)
	}
	var q Quad
	for {
		q, err = src.ReadQuad()
		if err == io.EOF {
			err = nil
			return
		} else if err != nil {
			return
		}
		if err = dst.WriteQuad(q); err != nil {
			return
		}
		n++
	}
}

type batchReader struct {
	Reader
}

func (r batchReader) ReadQuads(quads []Quad) (n int, err error) {
	for ; n < len(quads); n++ {
		quads[n], err = r.ReadQuad()
		if err != nil {
			break
		}
	}
	return
}

var DefaultBatch = 10000

// CopyBatch will copy all quads from src to dst in a batches of batchSize.
// It returns copied quads count and an error, if it failed.
//
// If batchSize <= 0 default batch size will be used.
func CopyBatch(dst BatchWriter, src Reader, batchSize int) (cnt int, err error) {
	if batchSize <= 0 {
		batchSize = DefaultBatch
	}
	buf := make([]Quad, batchSize)
	bsrc, ok := src.(BatchReader)
	if !ok {
		bsrc = batchReader{src}
	}
	var n int
	for err == nil {
		n, err = bsrc.ReadQuads(buf)
		if err != nil && err != io.EOF {
			return
		}
		eof := err == io.EOF
		n, err = dst.WriteQuads(buf[:n])
		cnt += n
		if eof {
			break
		}
	}
	return
}

// ReadAll reads all quads from r until EOF.
// It returns a slice with all quads that were read and an error, if any.
func ReadAll(r Reader) (arr []Quad, err error) {
	switch rt := r.(type) {
	case *Quads:
		arr = make([]Quad, len(rt.s))
		copy(arr, rt.s)
		rt.s = nil
		return
	}
	var q Quad
	for {
		q, err = r.ReadQuad()
		if err == io.EOF {
			return arr, nil
		} else if err != nil {
			return nil, err
		}
		arr = append(arr, q)
	}
}

// IRIOptions is a set of option
type IRIOptions struct {
	Format IRIFormat // short vs full IRI format
	// Func is executed after all other options and have a chance to replace the value.
	// Returning an empty IRI changes the value to nil.
	Func func(d Direction, iri IRI) (IRI, error)
}

// apply transforms the IRI using the specified options.
func (opt IRIOptions) apply(d Direction, v IRI) (IRI, error) {
	v = v.Format(opt.Format)
	if opt.Func != nil {
		var err error
		v, err = opt.Func(d, v)
		if err != nil {
			return "", err
		} else if v == "" {
			return "", nil
		}
	}
	return v, nil
}

// IRIWriter is a writer implementation that converts all IRI values in quads
// according to the IRIOptions.
func IRIWriter(w Writer, opt IRIOptions) Writer {
	return ValuesWriter(w, func(d Direction, v Value) (Value, error) {
		switch v := v.(type) {
		case IRI:
			var err error
			v, err = opt.apply(d, v)
			if err != nil {
				return nil, err
			} else if v == "" {
				return nil, nil
			}
			return v, nil
		case TypedString:
			var err error
			v.Type, err = opt.apply(d, v.Type)
			if err != nil {
				return nil, err
			}
			return v, nil
		}
		return v, nil
	})
}

// ValuesWriter is a writer implementation that converts all quad values using the callback function.
func ValuesWriter(w Writer, fnc func(d Direction, v Value) (Value, error)) Writer {
	return &valueWriter{w: w, fnc: fnc}
}

type valueWriter struct {
	w   Writer
	buf []Quad
	fnc func(d Direction, v Value) (Value, error)
}

func (w *valueWriter) apply(q *Quad) error {
	if v, err := w.fnc(Subject, q.Subject); err != nil {
		return err
	} else {
		q.Subject = v
	}
	if v, err := w.fnc(Predicate, q.Predicate); err != nil {
		return err
	} else {
		q.Predicate = v
	}
	if v, err := w.fnc(Object, q.Object); err != nil {
		return err
	} else {
		q.Object = v
	}
	if v, err := w.fnc(Label, q.Label); err != nil {
		return err
	} else {
		q.Label = v
	}
	return nil
}

func (w *valueWriter) WriteQuad(q Quad) error {
	if err := w.apply(&q); err != nil {
		return err
	}
	_, err := w.w.WriteQuads([]Quad{q})
	return err
}

func (w *valueWriter) WriteQuads(buf []Quad) (int, error) {
	w.buf = append(w.buf[:0], buf...)
	for i := range w.buf {
		if err := w.apply(&w.buf[i]); err != nil {
			return 0, err
		}
	}
	n, err := w.w.WriteQuads(w.buf)
	w.buf = w.buf[:0]
	return n, err
}
