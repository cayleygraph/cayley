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

// Defines the QuadStore interface. Every backing store must implement at
// least this interface.
//
// Most of these are pretty straightforward. As long as we can surface this
// interface, the rest of the stack will "just work" and we can connect to any
// quad backing store we prefer.

import (
	"errors"

	"github.com/barakmich/glog"
	"github.com/google/cayley/quad"
)

// Value defines an opaque "quad store value" type. However the backend wishes
// to implement it, a Value is merely a token to a quad or a node that the
// backing store itself understands, and the base iterators pass around.
//
// For example, in a very traditional, graphd-style graph, these are int64s
// (guids of the primitives). In a very direct sort of graph, these could be
// pointers to structs, or merely quads, or whatever works best for the
// backing store.
//
// These must be comparable, or implement a `Key() interface{}` function
// so that they may be stored in maps.
type Value interface{}

type QuadStore interface {
	// The only way in is through building a transaction, which
	// is done by a replication strategy.
	ApplyDeltas([]Delta) error

	// Given an opaque token, returns the quad for that token from the store.
	Quad(Value) quad.Quad

	// Given a direction and a token, creates an iterator of links which have
	// that node token in that directional field.
	QuadIterator(quad.Direction, Value) Iterator

	// Returns an iterator enumerating all nodes in the graph.
	NodesAllIterator() Iterator

	// Returns an iterator enumerating all links in the graph.
	QuadsAllIterator() Iterator

	// Given a node ID, return the opaque token used by the QuadStore
	// to represent that id.
	ValueOf(string) Value

	// Given an opaque token, return the node that it represents.
	NameOf(Value) string

	// Returns the number of quads currently stored.
	Size() int64

	// The last replicated transaction ID that this quadstore has verified.
	Horizon() PrimaryKey

	// Creates a fixed iterator which can compare Values
	FixedIterator() FixedIterator

	// Optimize an iterator in the context of the quad store.
	// Suppose we have a better index for the passed tree; this
	// gives the QuadStore the opportunity to replace it
	// with a more efficient iterator.
	OptimizeIterator(it Iterator) (Iterator, bool)

	// Close the quad store and clean up. (Flush to disk, cleanly
	// sever connections, etc)
	Close()

	// Convenience function for speed. Given a quad token and a direction
	// return the node token for that direction. Sometimes, a QuadStore
	// can do this without going all the way to the backing store, and
	// gives the QuadStore the opportunity to make this optimization.
	//
	// Iterators will call this. At worst, a valid implementation is
	//
	//  qs.ValueOf(qs.Quad(id).Get(dir))
	//
	QuadDirection(id Value, d quad.Direction) Value
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

func (d Options) BoolKey(key string) (bool, bool) {
	if val, ok := d[key]; ok {
		switch vv := val.(type) {
		case bool:
			return vv, true
		default:
			glog.Fatalln("Invalid", key, "parameter type from config.")
		}
	}
	return false, false
}

var ErrCannotBulkLoad = errors.New("quadstore: cannot bulk load")

type BulkLoader interface {
	// BulkLoad loads Quads from a quad.Unmarshaler in bulk to the QuadStore.
	// It returns ErrCannotBulkLoad if bulk loading is not possible. For example if
	// you cannot load in bulk to a non-empty database, and the db is non-empty.
	BulkLoad(quad.Unmarshaler) error
}

type NewStoreFunc func(string, Options) (QuadStore, error)
type InitStoreFunc func(string, Options) error

type register struct {
	newFunc      NewStoreFunc
	initFunc     InitStoreFunc
	isPersistent bool
}

var storeRegistry = make(map[string]register)

func RegisterQuadStore(name string, persists bool, newFunc NewStoreFunc, initFunc InitStoreFunc) {
	if _, found := storeRegistry[name]; found {
		panic("already registered QuadStore " + name)
	}
	storeRegistry[name] = register{
		newFunc:      newFunc,
		initFunc:     initFunc,
		isPersistent: persists,
	}
}

func NewQuadStore(name, dbpath string, opts Options) (QuadStore, error) {
	r, registered := storeRegistry[name]
	if !registered {
		return nil, errors.New("quadstore: name '" + name + "' is not registered")
	}
	return r.newFunc(dbpath, opts)
}

func InitQuadStore(name, dbpath string, opts Options) error {
	r, registered := storeRegistry[name]
	if registered {
		return r.initFunc(dbpath, opts)
	}
	return errors.New("quadstore: name '" + name + "' is not registered")
}

func IsPersistent(name string) bool {
	return storeRegistry[name].isPersistent
}

func QuadStores() []string {
	t := make([]string, 0, len(storeRegistry))
	for n := range storeRegistry {
		t = append(t, n)
	}
	return t
}
