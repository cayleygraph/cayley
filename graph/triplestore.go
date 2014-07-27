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

// Defines the TripleStore interface. Every backing store must implement at
// least this interface.
//
// Most of these are pretty straightforward. As long as we can surface this
// interface, the rest of the stack will "just work" and we can connect to any
// triple backing store we prefer.

import (
	"errors"

	"github.com/barakmich/glog"
	"github.com/google/cayley/quad"
)

// Defines an opaque "triple store value" type. However the backend wishes to
// implement it, a Value is merely a token to a triple or a node that the backing
// store itself understands, and the base iterators pass around.
//
// For example, in a very traditional, graphd-style graph, these are int64s
// (guids of the primitives). In a very direct sort of graph, these could be
// pointers to structs, or merely triples, or whatever works best for the
// backing store.
type Value interface{}

type TripleStore interface {
	// Add a triple to the store.
	AddTriple(*quad.Quad)

	// Add a set of triples to the store, atomically if possible.
	AddTripleSet([]*quad.Quad)

	// Removes a triple matching the given one  from the database,
	// if it exists. Does nothing otherwise.
	RemoveTriple(*quad.Quad)

	// Given an opaque token, returns the triple for that token from the store.
	Quad(Value) *quad.Quad

	// Given a direction and a token, creates an iterator of links which have
	// that node token in that directional field.
	TripleIterator(quad.Direction, Value) Iterator

	// Returns an iterator enumerating all nodes in the graph.
	NodesAllIterator() Iterator

	// Returns an iterator enumerating all links in the graph.
	TriplesAllIterator() Iterator

	// Given a node ID, return the opaque token used by the TripleStore
	// to represent that id.
	ValueOf(string) Value

	// Given an opaque token, return the node that it represents.
	NameOf(Value) string

	// Returns the number of triples currently stored.
	Size() int64

	// Creates a fixed iterator which can compare Values
	FixedIterator() FixedIterator

	// Optimize an iterator in the context of the triple store.
	// Suppose we have a better index for the passed tree; this
	// gives the TripleStore the opportunity to replace it
	// with a more efficient iterator.
	OptimizeIterator(it Iterator) (Iterator, bool)

	// Close the triple store and clean up. (Flush to disk, cleanly
	// sever connections, etc)
	Close()

	// Convenience function for speed. Given a triple token and a direction
	// return the node token for that direction. Sometimes, a TripleStore
	// can do this without going all the way to the backing store, and
	// gives the TripleStore the opportunity to make this optimization.
	//
	// Iterators will call this. At worst, a valid implementation is
	// ts.IdFor(ts.quad.Quad(id).Get(dir))
	TripleDirection(id Value, d quad.Direction) Value
}

type Options map[string]interface{}

func (d Options) IntKey(key string) (int, bool) {
	if val, ok := d[key]; ok {
		switch vv := val.(type) {
		case float64:
			return int(vv), true
		default:
			glog.Fatalln("Invalid", key, "parameter type from config.")
		}
	}
	return 0, false
}

func (d Options) StringKey(key string) (string, bool) {
	if val, ok := d[key]; ok {
		switch vv := val.(type) {
		case string:
			return vv, true
		default:
			glog.Fatalln("Invalid", key, "parameter type from config.")
		}
	}
	return "", false
}

var ErrCannotBulkLoad = errors.New("triplestore: cannot bulk load")

type BulkLoader interface {
	// BulkLoad loads Quads from a quad.Unmarshaler in bulk to the TripleStore.
	// It returns ErrCannotBulkLoad if bulk loading is not possible. For example if
	// you cannot load in bulk to a non-empty database, and the db is non-empty.
	BulkLoad(quad.Unmarshaler) error
}

type NewStoreFunc func(string, Options) (TripleStore, error)
type InitStoreFunc func(string, Options) error

var storeRegistry = make(map[string]NewStoreFunc)
var storeInitRegistry = make(map[string]InitStoreFunc)

func RegisterTripleStore(name string, newFunc NewStoreFunc, initFunc InitStoreFunc) {
	if _, found := storeRegistry[name]; found {
		panic("already registered TripleStore " + name)
	}
	storeRegistry[name] = newFunc
	if initFunc != nil {
		storeInitRegistry[name] = initFunc
	}
}

func NewTripleStore(name, dbpath string, opts Options) (TripleStore, error) {
	newFunc, hasNew := storeRegistry[name]
	if !hasNew {
		return nil, errors.New("triplestore: name '" + name + "' is not registered")
	}
	return newFunc(dbpath, opts)
}

func InitTripleStore(name, dbpath string, opts Options) error {
	initFunc, hasInit := storeInitRegistry[name]
	if hasInit {
		return initFunc(dbpath, opts)
	}
	if _, isRegistered := storeRegistry[name]; isRegistered {
		return nil
	}
	return errors.New("triplestore: name '" + name + "' is not registered")
}

func TripleStores() []string {
	t := make([]string, 0, len(storeRegistry))
	for n := range storeRegistry {
		t = append(t, n)
	}
	return t
}
