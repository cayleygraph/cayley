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
// Separate from the backend, this dictates how individual triples get
// identified and replicated consistently across (potentially) multiple
// instances. The simplest case is to keep an append-only log of triple
// changes.

import (
	"errors"
	"time"
)

type Procedure byte

// The different types of actions a transaction can do.
const (
	Add Procedure = iota
	Delete
)

type Transaction struct {
	ID        int64
	Triple    *Triple
	Action    Procedure
	Timestamp *time.Time
}

type TripleWriter interface {
	// Add a triple to the store.
	AddTriple(*Triple) error

	// Add a set of triples to the store, atomically if possible.
	AddTripleSet([]*Triple) error

	// Removes a triple matching the given one  from the database,
	// if it exists. Does nothing otherwise.
	RemoveTriple(*Triple) error
}

type NewTripleWriterFunc func(TripleStore, Options) (TripleWriter, error)

var writerRegistry = make(map[string]NewTripleWriterFunc)

func RegisterWriter(name string, newFunc NewTripleWriterFunc) {
	if _, found := writerRegistry[name]; found {
		panic("already registered TripleWriter " + name)
	}
	writerRegistry[name] = newFunc
}

func NewTripleWriter(name string, ts TripleStore, opts Options) (TripleWriter, error) {
	newFunc, hasNew := writerRegistry[name]
	if !hasNew {
		return nil, errors.New("replication: name '" + name + "' is not registered")
	}
	return newFunc(ts, opts)
}

func WriterMethods() []string {
	t := make([]string, 0, len(writerRegistry))
	for n := range writerRegistry {
		t = append(t, n)
	}
	return t
}
