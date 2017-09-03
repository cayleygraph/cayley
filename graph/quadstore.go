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
	"reflect"

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
// These must be comparable, or return a comparable version on Key.
type Value interface {
	Key() interface{}
}

// PreFetchedValue is an optional interface for graph.Value to indicate that
// quadstore has already loaded a value into memory.
type PreFetchedValue interface {
	Value
	NameOf() quad.Value
}

func PreFetched(v quad.Value) PreFetchedValue {
	return fetchedValue{v}
}

type fetchedValue struct {
	Val quad.Value
}

func (v fetchedValue) IsNode() bool       { return true }
func (v fetchedValue) NameOf() quad.Value { return v.Val }
func (v fetchedValue) Key() interface{}   { return v.Val }

// Keyer provides a method for comparing types that are not otherwise comparable.
// The Key method must return a dynamic type that is comparable according to the
// Go language specification. The returned value must be unique for each receiver
// value.
//
// Deprecated: Value contains the same method now.
type Keyer interface {
	Key() interface{}
}

// ToKey prepares Value to be stored inside maps, calling Key() if necessary.
func ToKey(v Value) interface{} {
	if v == nil {
		return nil
	}
	return v.Key()
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
	Close() error

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

var (
	typeInt = reflect.TypeOf(int(0))
)

func (d Options) IntKey(key string) (int, bool, error) {
	if val, ok := d[key]; ok {
		if reflect.TypeOf(val).ConvertibleTo(typeInt) {
			i := reflect.ValueOf(val).Convert(typeInt).Int()
			return int(i), true, nil
		}

		return 0, false, fmt.Errorf("Invalid %s parameter type from config: %T", key, val)
	}

	return 0, false, nil
}

func (d Options) StringKey(key string) (string, bool, error) {
	if val, ok := d[key]; ok {
		if v, ok := val.(string); ok {
			return v, true, nil
		}

		return "", false, fmt.Errorf("Invalid %s parameter type from config: %T", key, val)
	}

	return "", false, nil
}

func (d Options) BoolKey(key string) (bool, bool, error) {
	if val, ok := d[key]; ok {
		if v, ok := val.(bool); ok {
			return v, true, nil
		}

		return false, false, fmt.Errorf("Invalid %s parameter type from config: %T", key, val)
	}

	return false, false, nil
}

var ErrCannotBulkLoad = errors.New("quadstore: cannot bulk load")
var ErrDatabaseExists = errors.New("quadstore: cannot init; database already exists")

type BulkLoader interface {
	// BulkLoad loads Quads from a quad.Unmarshaler in bulk to the QuadStore.
	// It returns ErrCannotBulkLoad if bulk loading is not possible. For example if
	// you cannot load in bulk to a non-empty database, and the db is non-empty.
	BulkLoad(quad.Reader) error
}
