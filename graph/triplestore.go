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
	"github.com/barakmich/glog"
)

// Defines an opaque "triple store value" type. However the backend wishes to
// implement it, a TSVal is merely a token to a triple or a node that the backing
// store itself understands, and the base iterators pass around.
//
// For example, in a very traditional, graphd-style graph, these are int64s
// (guids of the primitives). In a very direct sort of graph, these could be
// pointers to structs, or merely triples, or whatever works best for the
// backing store.
type TSVal interface{}

type TripleStore interface {
	// Add a triple to the store.
	AddTriple(*Triple)

	// Add a set of triples to the store, atomically if possible.
	AddTripleSet([]*Triple)

	// Removes a triple matching the given one  from the database,
	// if it exists. Does nothing otherwise.
	RemoveTriple(*Triple)

	// Given an opaque token, returns the triple for that token from the store.
	GetTriple(TSVal) *Triple

	// Given a direction and a token, creates an iterator of links which have
	// that node token in that directional field.
	GetTripleIterator(string, TSVal) Iterator

	// Returns an iterator enumerating all nodes in the graph.
	GetNodesAllIterator() Iterator

	// Returns an iterator enumerating all links in the graph.
	GetTriplesAllIterator() Iterator

	// Given a node ID, return the opaque token used by the TripleStore
	// to represent that id.
	GetIdFor(string) TSVal

	// Given an opaque token, return the node that it represents.
	GetNameFor(TSVal) string

	// Returns the number of triples currently stored.
	Size() int64

	// Creates a Fixed iterator which can compare TSVals
	MakeFixed() *FixedIterator

	// Optimize an iterator in the context of the triple store.
	// Suppose we have a better index for the passed tree; this
	// gives the TripleStore the oppotunity to replace it
	// with a more efficient iterator.
	OptimizeIterator(it Iterator) (Iterator, bool)

	// Close the triple store and clean up. (Flush to disk, cleanly
	// sever connections, etc)
	Close()

	// Convienence function for speed. Given a triple token and a direction
	// return the node token for that direction. Sometimes, a TripleStore
	// can do this without going all the way to the backing store, and
	// gives the TripleStore the opportunity to make this optimization.
	//
	// Iterators will call this. At worst, a valid implementation is
	// self.GetIdFor(self.GetTriple(triple_id).Get(dir))
	GetTripleDirection(triple_id TSVal, dir string) TSVal
}

type OptionsDict map[string]interface{}

func (d OptionsDict) GetIntKey(key string) (int, bool) {
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

func (d OptionsDict) GetStringKey(key string) (string, bool) {
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
