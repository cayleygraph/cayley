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

// Package nquads is deprecated. Use github.com/cayleygraph/quad/nquads.
package nquads

import (
	"io"

	"github.com/cayleygraph/quad/nquads"
)

// AutoConvertTypedString allows to convert TypedString values to native
// equivalents directly while parsing. It will call ToNative on all TypedString values.
//
// If conversion error occurs, it will preserve original TypedString value.
//
// Deprecated: use github.com/cayleygraph/quad/nquads package instead.
var AutoConvertTypedString = nquads.AutoConvertTypedString

var DecodeRaw = nquads.DecodeRaw

// Reader implements N-Quad document parsing according to the RDF
// 1.1 N-Quads specification.
//
// Deprecated: use github.com/cayleygraph/quad/nquads package instead.
type Reader = nquads.Reader

// NewReader returns an N-Quad decoder that takes its input from the
// provided io.Reader.
//
// Deprecated: use github.com/cayleygraph/quad/nquads package instead.
func NewReader(r io.Reader, raw bool) *Reader {
	return nquads.NewReader(r, raw)
}

// NewWriter returns an N-Quad encoder that writes its output to the
// provided io.Writer.
//
// Deprecated: use github.com/cayleygraph/quad/nquads package instead.
func NewWriter(w io.Writer) *Writer { return nquads.NewWriter(w) }

// Writer implements N-Quad document generator according to the RDF
// 1.1 N-Quads specification.
//
// Deprecated: use github.com/cayleygraph/quad/nquads package instead.
type Writer = nquads.Writer
