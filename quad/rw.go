package quad

import (
	"io"
)

// Writer is a minimal interface for quad writers. Used for quad serializers and quad stores.
type Writer interface {
	WriteQuad(Quad) error
}

type WriteCloser interface {
	Writer
	io.Closer
}

// BatchWriter is an interface for writing quads in batches.
//
// WriteQuads returns a number of quads that where written and an error, if any.
type BatchWriter interface {
	WriteQuads(buf []Quad) (int, error)
}

// Reader is a minimal interface for quad readers. Used for quad deserializers and quad iterators.
//
// ReadQuad reads next valid Quad. It returns io.EOF if no quads are left.
type Reader interface {
	ReadQuad() (Quad, error)
}

// Skiper is an interface for quad reader that can skip quads efficiently without decoding them.
//
// It returns io.EOF if no quads are left.
type Skiper interface {
	SkipQuad() error
}

type ReadCloser interface {
	Reader
	io.Closer
}

// BatchReader is an interface for reading quads in batches.
//
// ReadQuads reads at most len(buf) quads into buf. It returns number of quads that were read and an error.
// It returns an io.EOF if there is no more quads to read.
type BatchReader interface {
	ReadQuads(buf []Quad) (int, error)
}

type sliceReader struct {
	s []Quad
	n int
}

func (r *sliceReader) ReadQuad() (Quad, error) {
	if r == nil || len(r.s) <= r.n {
		return Quad{}, io.EOF
	}
	q := r.s[r.n]
	r.n++
	return q, nil
}

// NewReader creates a quad reader from a quad slice.
func NewReader(quads []Quad) Reader {
	return &sliceReader{s: quads}
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
	case *sliceReader:
		arr = make([]Quad, len(rt.s)-rt.n)
		copy(arr, rt.s[rt.n:])
		rt.n = len(rt.s)
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
