package quad

import (
	"github.com/cayleygraph/quad"
)

// Writer is a minimal interface for quad writers. Used for quad serializers and quad stores.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
type Writer = quad.Writer

type WriteCloser = quad.WriteCloser

// BatchWriter is an interface for writing quads in batches.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
type BatchWriter = quad.BatchWriter

// Reader is a minimal interface for quad readers. Used for quad deserializers and quad iterators.
//
// ReadQuad reads next valid Quad. It returns io.EOF if no quads are left.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
type Reader = quad.Reader

// Skipper is an interface for quad reader that can skip quads efficiently without decoding them.
//
// It returns io.EOF if no quads are left.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
type Skipper = quad.Skipper

type ReadCloser = quad.ReadCloser

type ReadSkipCloser = quad.ReadSkipCloser

// BatchReader is an interface for reading quads in batches.
//
// ReadQuads reads at most len(buf) quads into buf. It returns number of quads that were read and an error.
// It returns an io.EOF if there is no more quads to read.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
type BatchReader = quad.BatchReader

type Quads = quad.Quads

// NewReader creates a quad reader from a quad slice.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
func NewReader(quads []Quad) *Quads {
	return quad.NewReader(quads)
}

// Copy will copy all quads from src to dst. It returns copied quads count and an error, if it failed.
//
// Copy will try to cast dst to BatchWriter and will switch to CopyBatch implementation in case of success.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
func Copy(dst Writer, src Reader) (n int, err error) {
	return quad.Copy(dst, src)
}

var DefaultBatch = quad.DefaultBatch

// CopyBatch will copy all quads from src to dst in a batches of batchSize.
// It returns copied quads count and an error, if it failed.
//
// If batchSize <= 0 default batch size will be used.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
func CopyBatch(dst BatchWriter, src Reader, batchSize int) (int, error) {
	return quad.CopyBatch(dst, src, batchSize)
}

// ReadAll reads all quads from r until EOF.
// It returns a slice with all quads that were read and an error, if any.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
func ReadAll(r Reader) ([]Quad, error) {
	return quad.ReadAll(r)
}

// IRIOptions is a set of option
//
// Deprecated: use github.com/cayleygraph/quad package instead.
type IRIOptions = quad.IRIOptions

// IRIWriter is a writer implementation that converts all IRI values in quads
// according to the IRIOptions.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
func IRIWriter(w Writer, opt IRIOptions) Writer {
	return quad.IRIWriter(w, opt)
}

// ValuesWriter is a writer implementation that converts all quad values using the callback function.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
func ValuesWriter(w Writer, fnc func(d Direction, v Value) (Value, error)) Writer {
	return quad.ValuesWriter(w, fnc)
}
