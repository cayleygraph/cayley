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

// Defines the struct which makes the TripleStore possible -- the triple.
//
// At its heart, it consists of three fields -- Subject, Predicate, and Object.
// Three IDs that relate to each other. That's all there is to it. The triples
// are the links in the graph, and the existence of node IDs is defined by the
// fact that some triple in the graph mentions them.
//
// This means that a complete representation of the graph is equivalent to a
// list of triples. The rest is just indexing for speed.
//
// Adding fields to the triple is not to be taken lightly. You'll see I mention
// provenance, but don't as yet use it in any backing store. In general, there
// can be features that can be turned on or off for any store, but I haven't
// decided how to allow/disallow them yet. Another such example would be to add
// a forward and reverse index field -- forward being "order the list of
// objects pointed at by this subject with this predicate" such as first and
// second children, top billing, what have you.
//
// There will never be that much in this file except for the definition, but
// the consequences are not to be taken lightly. But do suggest cool features!

import (
	"fmt"
	"reflect"
)

// Our triple struct, used throughout.
type Triple struct {
	Sub        string `json:"subject"`
	Pred       string `json:"predicate"`
	Obj        string `json:"object"`
	Provenance string `json:"provenance,omitempty"`
}

func NewTriple() *Triple {
	return &Triple{}
}

func MakeTriple(sub string, pred string, obj string, provenance string) *Triple {
	return &Triple{sub, pred, obj, provenance}
}

// List of the valid directions of a triple.
// TODO(barakmich): Replace all instances of "dir string" in the codebase
// with an enum of valid directions, to make this less stringly typed.
var TripleDirections = [4]string{"s", "p", "o", "c"}

// Per-field accessor for triples
func (t *Triple) Get(dir string) string {
	if dir == "s" {
		return t.Sub
	} else if dir == "p" {
		return t.Pred
	} else if dir == "prov" || dir == "c" {
		return t.Provenance
	} else if dir == "o" {
		return t.Obj
	} else {
		panic(fmt.Sprintf("No Such Triple Direction, %s", dir))
	}
}

func (t *Triple) Equals(other *Triple) bool {
	return reflect.DeepEqual(t, other)
}

// Pretty-prints a triple.
func (t *Triple) ToString() string {
	return fmt.Sprintf("%s -- %s -> %s\n", t.Sub, t.Pred, t.Obj)
}

func (t *Triple) IsValid() bool {
	if t.Sub == "" {
		return false
	}
	if t.Pred == "" {
		return false
	}
	if t.Obj == "" {
		return false
	}
	return true
}

// Prints a triple in N-Triple format.
func (t *Triple) ToNTriple() string {
	if t.Provenance == "" {
		//TODO(barakmich): Proper escaping.
		return fmt.Sprintf("%s %s %s .", t.Sub, t.Pred, t.Obj)
	} else {
		return fmt.Sprintf("%s %s %s %s .", t.Sub, t.Pred, t.Obj, t.Provenance)
	}
}
