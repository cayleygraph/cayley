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

	"github.com/cayleygraph/quad"
)

type BatchQuadStore interface {
	ValuesOf(ctx context.Context, vals []Ref) ([]quad.Value, error)
	RefsOf(ctx context.Context, nodes []quad.Value) ([]Ref, error)
}

func ValuesOf(ctx context.Context, qs Namer, vals []Ref) ([]quad.Value, error) {
	if bq, ok := qs.(BatchQuadStore); ok {
		return bq.ValuesOf(ctx, vals)
	}
	out := make([]quad.Value, len(vals))
	for i, v := range vals {
		out[i] = qs.NameOf(v)
	}
	return out, nil
}

func RefsOf(ctx context.Context, qs QuadStore, nodes []quad.Value) ([]Ref, error) {
	if bq, ok := qs.(BatchQuadStore); ok {
		return bq.RefsOf(ctx, nodes)
	}
	values := make([]Ref, len(nodes))
	for i, node := range nodes {
		value := qs.ValueOf(node)
		if value == nil {
			return nil, fmt.Errorf("not found: %v", node)
		}
		values[i] = value
	}
	return values, nil
}

type Namer interface {
	// Given a node ID, return the opaque token used by the QuadStore
	// to represent that id.
	ValueOf(quad.Value) Ref
	// Given an opaque token, return the node that it represents.
	NameOf(Ref) quad.Value
}

type QuadIndexer interface {
	// Given an opaque token, returns the quad for that token from the store.
	Quad(Ref) quad.Quad

	// Given a direction and a token, creates an iterator of links which have
	// that node token in that directional field.
	QuadIterator(quad.Direction, Ref) Iterator

	// QuadIteratorSize returns an estimated size of an iterator.
	QuadIteratorSize(ctx context.Context, d quad.Direction, v Ref) (Size, error)

	// Convenience function for speed. Given a quad token and a direction
	// return the node token for that direction. Sometimes, a QuadStore
	// can do this without going all the way to the backing store, and
	// gives the QuadStore the opportunity to make this optimization.
	//
	// Iterators will call this. At worst, a valid implementation is
	//
	//  qs.ValueOf(qs.Quad(id).Get(dir))
	//
	QuadDirection(id Ref, d quad.Direction) Ref
}

// Size of a graph (either in nodes or quads).
type Size struct {
	Size  int64
	Exact bool
}

// Stats of a graph.
type Stats struct {
	Nodes Size // number of nodes
	Quads Size // number of quads
}

type QuadStore interface {
	Namer
	QuadIndexer

	// The only way in is through building a transaction, which
	// is done by a replication strategy.
	ApplyDeltas(in []Delta, opts IgnoreOpts) error

	// NewQuadWriter starts a batch quad import process.
	// The order of changes is not guaranteed, neither is the order and result of concurrent ApplyDeltas.
	NewQuadWriter() (quad.WriteCloser, error)

	// Returns an iterator enumerating all nodes in the graph.
	NodesAllIterator() Iterator

	// Returns an iterator enumerating all links in the graph.
	QuadsAllIterator() Iterator

	// Stats returns the number of nodes and quads currently stored.
	// Exact flag controls the correctness of the value. It can be an estimation, or a precise calculation.
	// The quadstore may have a fast way of retrieving the precise stats, in this case it may ignore 'exact'
	// flag and always return correct stats (with an appropriate flags set in the output).
	Stats(ctx context.Context, exact bool) (Stats, error)

	// Close the quad store and clean up. (Flush to disk, cleanly
	// sever connections, etc)
	Close() error
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
