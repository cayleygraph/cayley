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

import "fmt"

// TODO(kortschak) Consider providing MashalJSON and UnmarshalJSON
// instead of using struct tags.

// Our triple struct, used throughout.
type Triple struct {
	Subject    string `json:"subject"`
	Predicate  string `json:"predicate"`
	Object     string `json:"object"`
	Provenance string `json:"provenance,omitempty"`
}

// Direction specifies an edge's type.
type Direction byte

// List of the valid directions of a triple.
const (
	Any Direction = iota
	Subject
	Predicate
	Object
	Provenance
)

func (d Direction) Prefix() byte {
	switch d {
	case Any:
		return 'a'
	case Subject:
		return 's'
	case Predicate:
		return 'p'
	case Provenance:
		return 'c'
	case Object:
		return 'o'
	default:
		return '\x00'
	}
}

func (d Direction) String() string {
	switch d {
	case Any:
		return "any"
	case Subject:
		return "subject"
	case Predicate:
		return "predicate"
	case Provenance:
		return "provenance"
	case Object:
		return "object"
	default:
		return fmt.Sprint("illegal direction:", byte(d))
	}
}

// TODO(kortschak) Consider writing methods onto the concrete type
// instead of the pointer. This needs benchmarking to make the decision.

// Per-field accessor for triples
func (t *Triple) Get(d Direction) string {
	switch d {
	case Subject:
		return t.Subject
	case Predicate:
		return t.Predicate
	case Provenance:
		return t.Provenance
	case Object:
		return t.Object
	default:
		panic(d.String())
	}
}

func (t *Triple) Equals(o *Triple) bool {
	return *t == *o
}

// Pretty-prints a triple.
func (t *Triple) String() string {
	return fmt.Sprintf("%s -- %s -> %s\n", t.Subject, t.Predicate, t.Object)
}

func (t *Triple) IsValid() bool {
	return t.Subject != "" && t.Predicate != "" && t.Object != ""
}

// TODO(kortschak) NTriple looks like a good candidate for conversion
// to MarshalText() (text []byte, err error) and then move parsing code
// from nquads to here to provide UnmarshalText(text []byte) error.

// Prints a triple in N-Triple format.
func (t *Triple) NTriple() string {
	if t.Provenance == "" {
		//TODO(barakmich): Proper escaping.
		return fmt.Sprintf("%s %s %s .", t.Subject, t.Predicate, t.Object)
	} else {
		return fmt.Sprintf("%s %s %s %s .", t.Subject, t.Predicate, t.Object, t.Provenance)
	}
}
