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

// Package quad is deprecated. Use github.com/cayleygraph/quad.
package quad

import (
	"github.com/cayleygraph/quad"
)

var (
	ErrInvalid    = quad.ErrInvalid
	ErrIncomplete = quad.ErrIncomplete
)

// Make creates a quad with provided values.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
func Make(subject, predicate, object, label interface{}) Quad {
	return quad.Make(subject, predicate, object, label)
}

// MakeRaw creates a quad with provided raw values (nquads-escaped).
//
// Deprecated: use Make pr MakeIRI instead.
func MakeRaw(subject, predicate, object, label string) Quad {
	return quad.MakeRaw(subject, predicate, object, label)
}

// MakeIRI creates a quad with provided IRI values.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
func MakeIRI(subject, predicate, object, label string) Quad {
	return quad.MakeIRI(subject, predicate, object, label)
}

// Our quad struct, used throughout.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
type Quad = quad.Quad

// Direction specifies an edge's type.
//
// Deprecated: use github.com/cayleygraph/quad package instead.
type Direction = quad.Direction

// List of the valid directions of a quad.
const (
	Any       = quad.Any
	Subject   = quad.Subject
	Predicate = quad.Predicate
	Object    = quad.Object
	Label     = quad.Label
)

var Directions = quad.Directions

type ByQuadString = quad.ByQuadString
