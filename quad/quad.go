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

// Package quad defines quad and triple handling.
package quad

// Defines the struct which makes the QuadStore possible -- the quad.
//
// At its heart, it consists of three fields -- Subject, Predicate, and Object.
// Three IDs that relate to each other. That's all there is to it. The quads
// are the links in the graph, and the existence of node IDs is defined by the
// fact that some quad in the graph mentions them.
//
// This means that a complete representation of the graph is equivalent to a
// list of quads. The rest is just indexing for speed.
//
// Adding fields to the quad is not to be taken lightly. You'll see I mention
// label, but don't as yet use it in any backing store. In general, there
// can be features that can be turned on or off for any store, but I haven't
// decided how to allow/disallow them yet. Another such example would be to add
// a forward and reverse index field -- forward being "order the list of
// objects pointed at by this subject with this predicate" such as first and
// second children, top billing, what have you.
//
// There will never be that much in this file except for the definition, but
// the consequences are not to be taken lightly. But do suggest cool features!

import (
	"errors"
	"fmt"
)

var (
	ErrInvalid    = errors.New("invalid N-Quad")
	ErrIncomplete = errors.New("incomplete N-Quad")
)

// Our quad struct, used throughout.
type Quad struct {
	Subject   string `json:"subject"`
	Predicate string `json:"predicate"`
	Object    string `json:"object"`
	Label     string `json:"label,omitempty"`
}

// Direction specifies an edge's type.
type Direction byte

// List of the valid directions of a quad.
const (
	Any Direction = iota
	Subject
	Predicate
	Object
	Label
)

var Directions = []Direction{Subject, Predicate, Object, Label}

func (d Direction) Prefix() byte {
	switch d {
	case Any:
		return 'a'
	case Subject:
		return 's'
	case Predicate:
		return 'p'
	case Label:
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
	case Label:
		return "label"
	case Object:
		return "object"
	default:
		return fmt.Sprint("illegal direction:", byte(d))
	}
}

// Per-field accessor for quads.
func (q Quad) Get(d Direction) string {
	switch d {
	case Subject:
		return q.Subject
	case Predicate:
		return q.Predicate
	case Label:
		return q.Label
	case Object:
		return q.Object
	default:
		panic(d.String())
	}
}

// Pretty-prints a quad.
func (q Quad) String() string {
	return fmt.Sprintf("%s -- %s -> %s", q.Subject, q.Predicate, q.Object)
}

func (q Quad) IsValid() bool {
	return q.Subject != "" && q.Predicate != "" && q.Object != ""
}

// Prints a quad in N-Quad format.
func (q Quad) NQuad() string {
	if q.Label == "" {
		//TODO(barakmich): Proper escaping.
		return fmt.Sprintf("%s %s %s .", q.Subject, q.Predicate, q.Object)
	}
	return fmt.Sprintf("%s %s %s %s .", q.Subject, q.Predicate, q.Object, q.Label)
}

type Unmarshaler interface {
	Unmarshal() (Quad, error)
}
