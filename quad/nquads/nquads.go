// Copyright 2014 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"

	"github.com/cayleygraph/cayley/quad"
)

//go:generate ragel -Z -G2 typed.rl
//go:generate ragel -Z -G2 raw.rl

// AutoConvertTypedString allows to convert TypedString values to native
// equivalents directly while parsing. It will call ToNative on all TypedString values.
//
// If conversion error occurs, it will preserve original TypedString value.
var AutoConvertTypedString = true

var DecodeRaw = false

func init() {
	quad.RegisterFormat(quad.Format{
		Name: "nquads",
		Ext:  []string{".nq", ".nt"},
		Mime: []string{"application/n-quads", "application/n-triples"},
		Reader: func(r io.Reader) quad.ReadCloser {
			return NewReader(r, DecodeRaw)
		},
		Writer: func(w io.Writer) quad.WriteCloser { return NewWriter(w) },
	})
}

// Reader implements N-Quad document parsing according to the RDF
// 1.1 N-Quads specification.
type Reader struct {
	r    *bufio.Reader
	line []byte
	raw  bool
}

// NewReader returns an N-Quad decoder that takes its input from the
// provided io.Reader.
func NewReader(r io.Reader, raw bool) *Reader {
	return &Reader{r: bufio.NewReader(r), raw: raw}
}

// ReadQuad returns the next valid N-Quad as a quad.Quad, or an error.
func (dec *Reader) ReadQuad() (quad.Quad, error) {
	dec.line = dec.line[:0]
	var line []byte
	for {
		for {
			l, pre, err := dec.r.ReadLine()
			if err != nil {
				return quad.Quad{}, err
			}
			dec.line = append(dec.line, l...)
			if !pre {
				break
			}
		}
		if line = bytes.TrimSpace(dec.line); len(line) != 0 && line[0] != '#' {
			break
		}
		dec.line = dec.line[:0]
	}
	var (
		q   quad.Quad
		err error
	)
	if dec.raw {
		q, err = ParseRaw(string(line))
	} else {
		q, err = Parse(string(line))
	}
	if err != nil {
		return quad.Quad{}, fmt.Errorf("failed to parse %q: %v", dec.line, err)
	}
	if !q.IsValid() {
		return dec.ReadQuad()
	}
	return q, nil
}
func (dec *Reader) Close() error { return nil }

func unEscape(r []rune, spec int, isQuoted, isEscaped bool) quad.Value {
	raw := r
	var sp []rune
	if spec > 0 {
		r, sp = r[:spec], r[spec:]
		isQuoted = true
	}
	if isQuoted {
		r = r[1 : len(r)-1]
	} else {
		if len(r) >= 2 && r[0] == '<' && r[len(r)-1] == '>' {
			return quad.IRI(r[1 : len(r)-1])
		}
		if len(r) >= 2 && r[0] == '_' && r[1] == ':' {
			return quad.BNode(string(r[2:]))
		}
	}
	var val string
	if isEscaped {
		buf := bytes.NewBuffer(make([]byte, 0, len(r)))

		for i := 0; i < len(r); {
			switch r[i] {
			case '\\':
				i++
				var c byte
				switch r[i] {
				case 't':
					c = '\t'
				case 'b':
					c = '\b'
				case 'n':
					c = '\n'
				case 'r':
					c = '\r'
				case 'f':
					c = '\f'
				case '"':
					c = '"'
				case '\'':
					c = '\''
				case '\\':
					c = '\\'
				case 'u':
					rc, err := strconv.ParseInt(string(r[i+1:i+5]), 16, 32)
					if err != nil {
						panic(fmt.Errorf("internal parser error: %v", err))
					}
					buf.WriteRune(rune(rc))
					i += 5
					continue
				case 'U':
					rc, err := strconv.ParseInt(string(r[i+1:i+9]), 16, 32)
					if err != nil {
						panic(fmt.Errorf("internal parser error: %v", err))
					}
					buf.WriteRune(rune(rc))
					i += 9
					continue
				}
				buf.WriteByte(c)
			default:
				buf.WriteRune(r[i])
			}
			i++
		}
		val = buf.String()
	} else {
		val = string(r)
	}
	if len(sp) == 0 {
		if isQuoted {
			return quad.String(val)
		}
		return quad.Raw(val)
	}
	if sp[0] == '@' {
		return quad.LangString{
			Value: quad.String(val),
			Lang:  string(sp[1:]),
		}
	} else if len(sp) >= 4 && sp[0] == '^' && sp[1] == '^' && sp[2] == '<' && sp[len(sp)-1] == '>' {
		v := quad.TypedString{
			Value: quad.String(val),
			Type:  quad.IRI(sp[3 : len(sp)-1]),
		}
		if AutoConvertTypedString {
			nv, err := v.ParseValue()
			if err == nil {
				return nv
			}
		}
		return v
	}
	return quad.Raw(raw)
}

func unEscapeRaw(r []rune, isEscaped bool) quad.Value {
	if !isEscaped {
		return quad.Raw(string(r))
	}

	buf := bytes.NewBuffer(make([]byte, 0, len(r)))

	for i := 0; i < len(r); {
		switch r[i] {
		case '\\':
			i++
			var c byte
			switch r[i] {
			case 't':
				c = '\t'
			case 'b':
				c = '\b'
			case 'n':
				c = '\n'
			case 'r':
				c = '\r'
			case 'f':
				c = '\f'
			case '"':
				c = '"'
			case '\'':
				c = '\''
			case '\\':
				c = '\\'
			case 'u':
				rc, err := strconv.ParseInt(string(r[i+1:i+5]), 16, 32)
				if err != nil {
					panic(fmt.Errorf("internal parser error: %v", err))
				}
				buf.WriteRune(rune(rc))
				i += 5
				continue
			case 'U':
				rc, err := strconv.ParseInt(string(r[i+1:i+9]), 16, 32)
				if err != nil {
					panic(fmt.Errorf("internal parser error: %v", err))
				}
				buf.WriteRune(rune(rc))
				i += 9
				continue
			}
			buf.WriteByte(c)
		default:
			buf.WriteRune(r[i])
		}
		i++
	}

	return quad.Raw(buf.String())
}

// NewWriter returns an N-Quad encoder that writes its output to the
// provided io.Writer.
func NewWriter(w io.Writer) *Writer { return &Writer{w: w} }

// Writer implements N-Quad document generator according to the RDF
// 1.1 N-Quads specification.
type Writer struct {
	w   io.Writer
	err error
}

func (enc *Writer) writeValue(v quad.Value) {
	if enc.err != nil {
		return
	}
	_, enc.err = enc.w.Write([]byte(v.String() + " "))
}
func (enc *Writer) WriteQuad(q quad.Quad) error {
	enc.writeValue(q.Subject)
	enc.writeValue(q.Predicate)
	enc.writeValue(q.Object)
	if q.Label != nil {
		enc.writeValue(q.Label)
	}
	if enc.err != nil {
		return enc.err
	}
	_, enc.err = enc.w.Write([]byte(".\n"))
	return enc.err
}
func (enc *Writer) Close() error { return enc.err }
