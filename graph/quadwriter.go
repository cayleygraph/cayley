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
	"errors"
	"time"

	"github.com/cayleygraph/cayley/quad"
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
	ID        PrimaryKey
	Quad      quad.Quad
	Action    Procedure
	Timestamp time.Time
}

type Handle struct {
	QuadStore
	QuadWriter
}

type IgnoreOpts struct {
	IgnoreDup, IgnoreMissing bool
}

func (h *Handle) Close() {
	h.QuadStore.Close()
	h.QuadWriter.Close()
}

var (
	ErrQuadExists    = errors.New("quad exists")
	ErrQuadNotExist  = errors.New("quad does not exist")
	ErrInvalidAction = errors.New("invalid action")
)

// DeltaError records an error and the delta that caused it.
type DeltaError struct {
	Delta Delta
	Err   error
}

func (e *DeltaError) Error() string {
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
	IgnoreDuplicates = false

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
	RemoveNode(Value) error

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
