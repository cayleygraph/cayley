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
	"encoding/json"
	"errors"
	"fmt"
)

var (
	ErrInvalid    = errors.New("invalid N-Quad")
	ErrIncomplete = errors.New("incomplete N-Quad")
)

// Make creates a quad with provided values.
func Make(subject, predicate, object, label interface{}) (q Quad) {
	var ok bool
	if q.Subject, ok = AsValue(subject); !ok {
		q.Subject = String(fmt.Sprint(subject))
	}
	if q.Predicate, ok = AsValue(predicate); !ok {
		q.Predicate = String(fmt.Sprint(predicate))
	}
	if q.Object, ok = AsValue(object); !ok {
		q.Object = String(fmt.Sprint(object))
	}
	if q.Label, ok = AsValue(label); !ok {
		q.Label = String(fmt.Sprint(label))
	}
	return
}

// MakeRaw creates a quad with provided raw values (nquads-escaped).
//
// Deprecated: use Make pr MakeIRI instead.
func MakeRaw(subject, predicate, object, label string) (q Quad) {
	if subject != "" {
		q.Subject = Raw(subject)
	}
	if predicate != "" {
		q.Predicate = Raw(predicate)
	}
	if object != "" {
		q.Object = Raw(object)
	}
	if label != "" {
		q.Label = Raw(label)
	}
	return
}

// MakeIRI creates a quad with provided IRI values.
func MakeIRI(subject, predicate, object, label string) (q Quad) {
	if subject != "" {
		q.Subject = IRI(subject)
	}
	if predicate != "" {
		q.Predicate = IRI(predicate)
	}
	if object != "" {
		q.Object = IRI(object)
	}
	if label != "" {
		q.Label = IRI(label)
	}
	return
}

var (
	_ json.Marshaler   = Quad{}
	_ json.Unmarshaler = (*Quad)(nil)
)

// Our quad struct, used throughout.
type Quad struct {
	Subject   Value `json:"subject"`
	Predicate Value `json:"predicate"`
	Object    Value `json:"object"`
	Label     Value `json:"label,omitempty"`
}

type rawQuad struct {
	Subject   string `json:"subject"`
	Predicate string `json:"predicate"`
	Object    string `json:"object"`
	Label     string `json:"label,omitempty"`
}

func (q Quad) MarshalJSON() ([]byte, error) {
	rq := rawQuad{
		Subject:   ToString(q.Subject),
		Predicate: ToString(q.Predicate),
		Object:    ToString(q.Object),
	}
	if q.Label != nil {
		rq.Label = ToString(q.Label)
	}
	return json.Marshal(rq)
}
func (q *Quad) UnmarshalJSON(data []byte) error {
	var rq rawQuad
	if err := json.Unmarshal(data, &rq); err != nil {
		return err
	}
	// TODO(dennwc): parse nquads? or use StringToValue hack?
	*q = MakeRaw(rq.Subject, rq.Predicate, rq.Object, rq.Label)
	return nil
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

func (d Direction) GoString() string {
	switch d {
	case Any:
		return "quad.Any"
	case Subject:
		return "quad.Subject"
	case Predicate:
		return "quad.Predicate"
	case Label:
		return "quad.Label"
	case Object:
		return "quad.Object"
	default:
		return fmt.Sprintf("%x", byte(d))
	}
}

// Per-field accessor for quads.
func (q Quad) Get(d Direction) Value {
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

func (q *Quad) Set(d Direction, v Value) {
	switch d {
	case Subject:
		q.Subject = v
	case Predicate:
		q.Predicate = v
	case Label:
		q.Label = v
	case Object:
		q.Object = v
	default:
		panic(d.String())
	}
}

// Per-field accessor for quads that returns strings instead of values.
func (q Quad) GetString(d Direction) string {
	switch d {
	case Subject:
		return StringOf(q.Subject)
	case Predicate:
		return StringOf(q.Predicate)
	case Object:
		return StringOf(q.Object)
	case Label:
		return StringOf(q.Label)
	default:
		panic(d.String())
	}
}

// Pretty-prints a quad.
func (q Quad) String() string {
	return fmt.Sprintf("%v -- %v -> %v", q.Subject, q.Predicate, q.Object)
}

func (q Quad) IsValid() bool {
	return IsValidValue(q.Subject) && IsValidValue(q.Predicate) && IsValidValue(q.Object)
}

// Prints a quad in N-Quad format.
func (q Quad) NQuad() string {
	if q.Label == nil || q.Label.String() == "" {
		return fmt.Sprintf("%s %s %s .", q.Subject, q.Predicate, q.Object)
	}
	return fmt.Sprintf("%s %s %s %s .", q.Subject, q.Predicate, q.Object, q.Label)
}

type ByQuadString []Quad

func (o ByQuadString) Len() int { return len(o) }
func (o ByQuadString) Less(i, j int) bool {
	switch { // TODO: optimize
	case StringOf(o[i].Subject) < StringOf(o[j].Subject),

		StringOf(o[i].Subject) == StringOf(o[j].Subject) &&
			StringOf(o[i].Predicate) < StringOf(o[j].Predicate),

		StringOf(o[i].Subject) == StringOf(o[j].Subject) &&
			StringOf(o[i].Predicate) == StringOf(o[j].Predicate) &&
			StringOf(o[i].Object) < StringOf(o[j].Object),

		StringOf(o[i].Subject) == StringOf(o[j].Subject) &&
			StringOf(o[i].Predicate) == StringOf(o[j].Predicate) &&
			StringOf(o[i].Object) == StringOf(o[j].Object) &&
			StringOf(o[i].Label) < StringOf(o[j].Label):

		return true

	default:
		return false
	}
}
func (o ByQuadString) Swap(i, j int) { o[i], o[j] = o[j], o[i] }
