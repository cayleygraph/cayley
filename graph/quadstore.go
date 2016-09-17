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
	"fmt"

	"github.com/cayleygraph/cayley/quad"
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
// These must be comparable, or implement a Keyer interface
// so that they may be stored in maps.
type Value interface {
	IsNode() bool
}

// PreFetchedValue is an optional interface for graph.Value to indicate that
// quadstore has already loaded a value into memory.
type PreFetchedValue interface {
	NameOf() quad.Value
}

// Keyer provides a method for comparing types that are not otherwise comparable.
// The Key method must return a dynamic type that is comparable according to the
// Go language specification. The returned value must be unique for each receiver
// value.
type Keyer interface {
	Key() interface{}
}

type key struct {
	Val  interface{}
	Node bool
}

func (k key) IsNode() bool { return k.Node }

// ToKey prepares Value to be stored inside maps, calling Key() if necessary.
func ToKey(v Value) Value {
	if k, ok := v.(Keyer); ok {
		return key{
			Val:  k.Key(),
			Node: v.IsNode(),
		}
	}
	return v
}

type QuadStore interface {
	// The only way in is through building a transaction, which
	// is done by a replication strategy.
	ApplyDeltas([]Delta, IgnoreOpts) error

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
	ValueOf(quad.Value) Value

	// Given an opaque token, return the node that it represents.
	NameOf(Value) quad.Value

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

	// Get the type of QuadStore
	//TODO replace this using reflection
	Type() string
}

type Options map[string]interface{}

func (d Options) IntKey(key string) (int, bool, error) {
	if val, ok := d[key]; ok {
		switch vv := val.(type) {
		case float64:
			return int(vv), true, nil
		default:
			return 0, false, fmt.Errorf("Invalid %s parameter type from config: %T", key, val)
		}
	}
	return 0, false, nil
}

func (d Options) StringKey(key string) (string, bool, error) {
	if val, ok := d[key]; ok {
		switch vv := val.(type) {
		case string:
			return vv, true, nil
		default:
			return "", false, fmt.Errorf("Invalid %s parameter type from config: %T", key, val)
		}
	}
	return "", false, nil
}

func (d Options) BoolKey(key string) (bool, bool, error) {
	if val, ok := d[key]; ok {
		switch vv := val.(type) {
		case bool:
			return vv, true, nil
		default:
			return false, false, fmt.Errorf("Invalid %s parameter type from config: %T", key, val)
		}
	}
	return false, false, nil
}

var ErrCannotBulkLoad = errors.New("quadstore: cannot bulk load")
var ErrDatabaseExists = errors.New("quadstore: cannot init; database already exists")

type BulkLoader interface {
	// BulkLoad loads Quads from a quad.Unmarshaler in bulk to the QuadStore.
	// It returns ErrCannotBulkLoad if bulk loading is not possible. For example if
	// you cannot load in bulk to a non-empty database, and the db is non-empty.
	BulkLoad(quad.Unmarshaler) error
}

type NewStoreFunc func(string, Options) (QuadStore, error)
type InitStoreFunc func(string, Options) error
type UpgradeStoreFunc func(string, Options) error
type NewStoreForRequestFunc func(QuadStore, Options) (QuadStore, error)

type QuadStoreRegistration struct {
	NewFunc           NewStoreFunc
	NewForRequestFunc NewStoreForRequestFunc
	UpgradeFunc       UpgradeStoreFunc
	InitFunc          InitStoreFunc
	IsPersistent      bool
}

var storeRegistry = make(map[string]QuadStoreRegistration)

func RegisterQuadStore(name string, register QuadStoreRegistration) {
	if _, found := storeRegistry[name]; found {
		panic("already registered QuadStore " + name)
	}
	storeRegistry[name] = register
}

func NewQuadStore(name, dbpath string, opts Options) (QuadStore, error) {
	r, registered := storeRegistry[name]
	if !registered {
		return nil, errors.New("quadstore: name '" + name + "' is not registered")
	}
	return r.NewFunc(dbpath, opts)
}

func InitQuadStore(name, dbpath string, opts Options) error {
	r, registered := storeRegistry[name]
	if registered {
		return r.InitFunc(dbpath, opts)
	}
	return errors.New("quadstore: name '" + name + "' is not registered")
}

func NewQuadStoreForRequest(qs QuadStore, opts Options) (QuadStore, error) {
	r, registered := storeRegistry[qs.Type()]
	if registered {
		return r.NewForRequestFunc(qs, opts)
	}
	return nil, errors.New("QuadStore does not support Per Request construction, check config")
}

func UpgradeQuadStore(name, dbpath string, opts Options) error {
	r, registered := storeRegistry[name]
	if registered {
		if r.UpgradeFunc != nil {
			return r.UpgradeFunc(dbpath, opts)
		} else {
			return nil
		}
	}
	return errors.New("quadstore: name '" + name + "' is not registered")

}

func IsPersistent(name string) bool {
	return storeRegistry[name].IsPersistent
}

func QuadStores() []string {
	t := make([]string, 0, len(storeRegistry))
	for n := range storeRegistry {
		t = append(t, n)
	}
	return t
}
