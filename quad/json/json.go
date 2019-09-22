// Package json is deprecated. Use github.com/cayleygraph/quad/json.
package json

import (
	"io"

	"github.com/cayleygraph/quad/json"
)

func NewReader(r io.Reader) *Reader {
	return json.NewReader(r)
}

type Reader = json.Reader

func NewStreamReader(r io.Reader) *StreamReader {
	return json.NewStreamReader(r)
}

type StreamReader = json.StreamReader

func NewWriter(w io.Writer) *Writer {
	return json.NewWriter(w)
}

type Writer = json.Writer

func NewStreamWriter(w io.Writer) *StreamWriter {
	return json.NewStreamWriter(w)
}

type StreamWriter = json.StreamWriter
