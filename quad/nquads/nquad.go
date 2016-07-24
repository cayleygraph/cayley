// Package nquads implements parsing the RDF 1.1 N-Quads like line-based syntax
// for RDF datasets.
//
// Typed parsing is performed as based on a simplified grammar derived from
// the N-Quads grammar defined by http://www.w3.org/TR/n-quads/.
//
// Raw parsing is performed as defined by http://www.w3.org/TR/n-quads/
// with the exception that parser will allow relative IRI values,
// which are prohibited by the N-Quads quad-Quads specifications.
//
// For a complete definition of the grammar, see cquads.rl and nquads.rl.
package nquads

import (
	"github.com/cayleygraph/cayley/quad"
	"io"
)

var DecodeRaw = false

func init() {
	quad.RegisterFormat(quad.Format{
		Name: "nquads",
		Ext:  []string{".nq", ".nt"},
		Mime: []string{"application/n-quads", "application/n-triples"},
		Reader: func(r io.Reader) quad.ReadCloser {
			if DecodeRaw {
				return NewRawReader(r)
			}
			return NewReader(r)
		},
		Writer: func(w io.Writer) quad.WriteCloser { return NewWriter(w) },
	})
}
