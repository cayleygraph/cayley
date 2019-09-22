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

package graph

// Defines the interface for consistent replication of a graph instance.
//
// Separate from the backend, this dictates how individual quads get
// identified and replicated consistently across (potentially) multiple
// instances. The simplest case is to keep an append-only log of quad
// changes.

import (
	"context"
	"errors"
	"io"

	"github.com/cayleygraph/quad"
)

type Procedure int8

func (p Procedure) String() string {
	switch p {
	case +1:
		return "add"
	case -1:
		return "delete"
	default:
		return "invalid"
	}
}

// The different types of actions a transaction can do.
const (
	Add    Procedure = +1
	Delete Procedure = -1
)

type Delta struct {
	Quad   quad.Quad
	Action Procedure
}

// Unwrap returns an original QuadStore value if it was wrapped by Handle.
// This prevents shadowing of optional interface implementations.
func Unwrap(qs QuadStore) QuadStore {
	if h, ok := qs.(*Handle); ok {
		return h.QuadStore
	}
	return qs
}

type Handle struct {
	QuadStore
	QuadWriter
}

type IgnoreOpts struct {
	IgnoreDup, IgnoreMissing bool
}

func (h *Handle) Close() error {
	err := h.QuadWriter.Close()
	h.QuadStore.Close()
	return err
}

var (
	ErrQuadExists    = errors.New("quad exists")
	ErrQuadNotExist  = errors.New("quad does not exist")
	ErrInvalidAction = errors.New("invalid action")
	ErrNodeNotExists = errors.New("node does not exist")
)

// DeltaError records an error and the delta that caused it.
type DeltaError struct {
	Delta Delta
	Err   error
}

func (e *DeltaError) Error() string {
	if !e.Delta.Quad.IsValid() {
		return e.Err.Error()
	}
	return e.Delta.Action.String() + " " + e.Delta.Quad.String() + ": " + e.Err.Error()
}

// IsQuadExist returns whether an error is a DeltaError
// with the Err field equal to ErrQuadExists.
func IsQuadExist(err error) bool {
	if err == ErrQuadExists {
		return true
	}
	de, ok := err.(*DeltaError)
	return ok && de.Err == ErrQuadExists
}

// IsQuadNotExist returns whether an error is a DeltaError
// with the Err field equal to ErrQuadNotExist.
func IsQuadNotExist(err error) bool {
	if err == ErrQuadNotExist {
		return true
	}
	de, ok := err.(*DeltaError)
	return ok && de.Err == ErrQuadNotExist
}

// IsInvalidAction returns whether an error is a DeltaError
// with the Err field equal to ErrInvalidAction.
func IsInvalidAction(err error) bool {
	if err == ErrInvalidAction {
		return true
	}
	de, ok := err.(*DeltaError)
	return ok && de.Err == ErrInvalidAction
}

var (
	// IgnoreDuplicates specifies whether duplicate quads
	// cause an error during loading or are ignored.
	IgnoreDuplicates = true

	// IgnoreMissing specifies whether missing quads
	// cause an error during deletion or are ignored.
	IgnoreMissing = false
)

type QuadWriter interface {
	// AddQuad adds a quad to the store.
	AddQuad(quad.Quad) error

	// TODO(barakmich): Deprecate in favor of transaction.
	// AddQuadSet adds a set of quads to the store, atomically if possible.
	AddQuadSet([]quad.Quad) error

	// RemoveQuad removes a quad matching the given one  from the database,
	// if it exists. Does nothing otherwise.
	RemoveQuad(quad.Quad) error

	// ApplyTransaction applies a set of quad changes.
	ApplyTransaction(*Transaction) error

	// RemoveNode removes all quads which have the given node as subject, predicate, object, or label.
	//
	// It returns ErrNodeNotExists if node is missing.
	RemoveNode(quad.Value) error

	// Close cleans up replication and closes the writing aspect of the database.
	Close() error
}

type NewQuadWriterFunc func(QuadStore, Options) (QuadWriter, error)

var writerRegistry = make(map[string]NewQuadWriterFunc)

func RegisterWriter(name string, newFunc NewQuadWriterFunc) {
	if _, found := writerRegistry[name]; found {
		panic("already registered QuadWriter " + name)
	}
	writerRegistry[name] = newFunc
}

func NewQuadWriter(name string, qs QuadStore, opts Options) (QuadWriter, error) {
	newFunc, hasNew := writerRegistry[name]
	if !hasNew {
		return nil, errors.New("replication: name '" + name + "' is not registered")
	}
	return newFunc(qs, opts)
}

func WriterMethods() []string {
	t := make([]string, 0, len(writerRegistry))
	for n := range writerRegistry {
		t = append(t, n)
	}
	return t
}

type BatchWriter interface {
	quad.WriteCloser
	Flush() error
}

// NewWriter creates a quad writer for a given QuadStore.
//
// Caller must call Flush or Close to flush an internal buffer.
func NewWriter(qs QuadWriter) BatchWriter {
	return &batchWriter{qs: qs}
}

type batchWriter struct {
	qs  QuadWriter
	buf []quad.Quad
}

func (w *batchWriter) flushBuffer(force bool) error {
	if !force && len(w.buf) < quad.DefaultBatch {
		return nil
	}
	_, err := w.WriteQuads(w.buf)
	w.buf = w.buf[:0]
	return err
}

func (w *batchWriter) WriteQuad(q quad.Quad) error {
	if err := w.flushBuffer(false); err != nil {
		return err
	}
	w.buf = append(w.buf, q)
	return nil
}
func (w *batchWriter) WriteQuads(quads []quad.Quad) (int, error) {
	if err := w.qs.AddQuadSet(quads); err != nil {
		return 0, err
	}
	return len(quads), nil
}
func (w *batchWriter) Flush() error {
	return w.flushBuffer(true)
}
func (w *batchWriter) Close() error {
	return w.Flush()
}

// NewTxWriter creates a writer that applies a given procedures for all quads in stream.
// If procedure is zero, Add operation will be used.
func NewTxWriter(tx *Transaction, p Procedure) quad.Writer {
	if p == 0 {
		p = Add
	}
	return &txWriter{tx: tx, p: p}
}

type txWriter struct {
	tx *Transaction
	p  Procedure
}

func (w *txWriter) WriteQuad(q quad.Quad) error {
	switch w.p {
	case Add:
		w.tx.AddQuad(q)
	case Delete:
		w.tx.RemoveQuad(q)
	default:
		return ErrInvalidAction
	}
	return nil
}

func (w *txWriter) WriteQuads(buf []quad.Quad) (int, error) {
	for i, q := range buf {
		if err := w.WriteQuad(q); err != nil {
			return i, err
		}
	}
	return len(buf), nil
}

// NewRemover creates a quad writer for a given QuadStore which removes quads instead of adding them.
func NewRemover(qs QuadWriter) BatchWriter {
	return &removeWriter{qs: qs}
}

type removeWriter struct {
	qs QuadWriter
}

func (w *removeWriter) WriteQuad(q quad.Quad) error {
	return w.qs.RemoveQuad(q)
}
func (w *removeWriter) WriteQuads(quads []quad.Quad) (int, error) {
	tx := NewTransaction()
	for _, q := range quads {
		tx.RemoveQuad(q)
	}
	if err := w.qs.ApplyTransaction(tx); err != nil {
		return 0, err
	}
	return len(quads), nil
}
func (w *removeWriter) Flush() error {
	return nil // TODO: batch deletes automatically
}
func (w *removeWriter) Close() error { return nil }

// NewResultReader creates a quad reader for a given QuadStore.
func NewQuadStoreReader(qs QuadStore) quad.ReadSkipCloser {
	return NewResultReader(qs, nil)
}

// NewResultReader creates a quad reader for a given QuadStore and iterator.
// If iterator is nil QuadsAllIterator will be used.
//
// Only quads returned by iterator's Result will be used.
//
// Iterator will be closed with the reader.
func NewResultReader(qs QuadStore, it Iterator) quad.ReadSkipCloser {
	if it == nil {
		it = qs.QuadsAllIterator()
	}
	return &quadReader{qs: qs, it: it}
}

type quadReader struct {
	qs QuadStore
	it Iterator
}

func (r *quadReader) ReadQuad() (quad.Quad, error) {
	if r.it.Next(context.TODO()) {
		return r.qs.Quad(r.it.Result()), nil
	}
	err := r.it.Err()
	if err == nil {
		err = io.EOF
	}
	return quad.Quad{}, err
}
func (r *quadReader) SkipQuad() error {
	if r.it.Next(context.TODO()) {
		return nil
	}
	if err := r.it.Err(); err != nil {
		return err
	}
	return io.EOF
}
func (r *quadReader) Close() error { return r.it.Close() }
