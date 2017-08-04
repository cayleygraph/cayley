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

// Package query defines the graph session interface general to all query languages.
package query

import (
	"context"
	"errors"
	"io"

	"github.com/cayleygraph/cayley/graph"
)

var ErrParseMore = errors.New("query: more input required")

type Result interface {
	Result() interface{}
	Err() error
}

func ErrorResult(err error) Result {
	return errResult{err: err}
}

type errResult struct {
	err error
}

func (errResult) Result() interface{} { return nil }
func (e errResult) Err() error        { return e.err }

func TagMapResult(m map[string]graph.Value) Result {
	return tagMap(m)
}

type tagMap map[string]graph.Value

func (m tagMap) Result() interface{} { return map[string]graph.Value(m) }
func (tagMap) Err() error            { return nil }

type Session interface {
	// Runs the query and returns individual results on the channel.
	//
	// Channel will be closed when function returns.
	Execute(ctx context.Context, query string, out chan Result, limit int)
}

// TODO(dennwc): review HTTP interface (Collate is weird)
// TODO(dennwc): specify exact type to return from ShapeOf
// TODO(dennwc): add context to ShapeOf?

type HTTP interface {
	Session
	ShapeOf(string) (interface{}, error)
	Collate(Result)
	Results() (interface{}, error)
}

type REPLSession interface {
	Session
	FormatREPL(Result) string
}

// ResponseWriter is a subset of http.ResponseWriter
type ResponseWriter interface {
	Write([]byte) (int, error)
	WriteHeader(int)
}

// Language is a description of query language.
type Language struct {
	Name    string
	Session func(graph.QuadStore) Session
	REPL    func(graph.QuadStore) REPLSession
	HTTP    func(graph.QuadStore) HTTP

	// Custom HTTP handlers

	HTTPQuery func(ctx context.Context, qs graph.QuadStore, w ResponseWriter, r io.Reader)
	HTTPError func(w ResponseWriter, err error)
}

var languages = make(map[string]Language)

// RegisterLanguage register a new query language.
func RegisterLanguage(lang Language) {
	languages[lang.Name] = lang
}

// NewSession creates a new session for specified query language.
// It returns nil if language was not registered.
func NewSession(qs graph.QuadStore, lang string) Session {
	if l := languages[lang]; l.Session != nil {
		return l.Session(qs)
	}
	return nil
}

// GetLanguage returns a query language description.
// It returns nil if language was not registered.
func GetLanguage(lang string) *Language {
	l, ok := languages[lang]
	if ok {
		return &l
	}
	return nil
}

// Languages returns names of registered query languages.
func Languages() []string {
	out := make([]string, 0, len(languages))
	for name := range languages {
		out = append(out, name)
	}
	return out
}
