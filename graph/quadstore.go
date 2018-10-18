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
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/cayleygraph/cayley/quad"
)

type BatchQuadStore interface {
	ValuesOf(ctx context.Context, vals []Value) ([]quad.Value, error)
	RefsOf(ctx context.Context, nodes []quad.Value) ([]Value, error)
}

func ValuesOf(ctx context.Context, qs QuadStore, vals []Value) ([]quad.Value, error) {
	if bq, ok := qs.(BatchQuadStore); ok {
		return bq.ValuesOf(ctx, vals)
	}
	out := make([]quad.Value, len(vals))
	for i, v := range vals {
		out[i] = qs.NameOf(v)
	}
	return out, nil
}

func RefsOf(ctx context.Context, qs QuadStore, nodes []quad.Value) ([]Value, error) {
	if bq, ok := qs.(BatchQuadStore); ok {
		return bq.RefsOf(ctx, nodes)
	}
	values := make([]Value, len(nodes))
	for i, node := range nodes {
		value := qs.ValueOf(node)
		if value == nil {
			return nil, fmt.Errorf("not found: %v", node)
		}
		values[i] = value
	}
	return values, nil
}

type QuadStore interface {
	// The only way in is through building a transaction, which
	// is done by a replication strategy.
	ApplyDeltas(in []Delta, opts IgnoreOpts) error

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

func (d Options) IntKey(key string, def int) (int, error) {
	if val, ok := d[key]; ok {
		if reflect.TypeOf(val).ConvertibleTo(typeInt) {
			i := reflect.ValueOf(val).Convert(typeInt).Int()
			return int(i), nil
		}

		return def, fmt.Errorf("Invalid %s parameter type from config: %T", key, val)
	}
	return def, nil
}

func (d Options) StringKey(key string, def string) (string, error) {
	if val, ok := d[key]; ok {
		if v, ok := val.(string); ok {
			return v, nil
		}

		return def, fmt.Errorf("Invalid %s parameter type from config: %T", key, val)
	}

	return def, nil
}

func (d Options) BoolKey(key string, def bool) (bool, error) {
	if val, ok := d[key]; ok {
		if v, ok := val.(bool); ok {
			return v, nil
		}

		return def, fmt.Errorf("Invalid %s parameter type from config: %T", key, val)
	}

	return def, nil
}

var (
	ErrDatabaseExists = errors.New("quadstore: cannot init; database already exists")
	ErrNotInitialized = errors.New("quadstore: not initialized")
)

type BulkLoader interface {
	// BulkLoad loads Quads from a quad.Unmarshaler in bulk to the QuadStore.
	// It returns ErrCannotBulkLoad if bulk loading is not possible. For example if
	// you cannot load in bulk to a non-empty database, and the db is non-empty.
	BulkLoad(quad.Reader) error
}
