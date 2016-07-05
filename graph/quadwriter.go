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
	"flag"
	"time"

	"github.com/cayleygraph/cayley/quad"
)

type Procedure int8

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
	ErrQuadExists   = errors.New("quad exists")
	ErrQuadNotExist = errors.New("quad does not exist")
)

var (
	IgnoreDup     = flag.Bool("ignoredup", false, "Don't stop loading on duplicated key on add")
	IgnoreMissing = flag.Bool("ignoremissing", false, "Don't stop loading on missing key on delete")
)

type QuadWriter interface {
	// Add a quad to the store.
	AddQuad(quad.Quad) error

	// TODO(barakmich): Deprecate in favor of transaction.
	// Add a set of quads to the store, atomically if possible.
	AddQuadSet([]quad.Quad) error

	// Removes a quad matching the given one  from the database,
	// if it exists. Does nothing otherwise.
	RemoveQuad(quad.Quad) error

	// Apply a set of quad changes
	ApplyTransaction(*Transaction) error

	// Cleans up replication and closes the writing aspect of the database.
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
