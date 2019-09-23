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
	"fmt"
	"io"

	"github.com/cayleygraph/cayley/graph"
)

var ErrParseMore = errors.New("query: more input required")

type ErrUnsupportedCollation struct {
	Collation Collation
}

func (e *ErrUnsupportedCollation) Error() string {
	return fmt.Sprintf("unsupported collation: %v", e.Collation)
}

// Iterator for query results.
type Iterator interface {
	// Next advances the iterator to the next value, which will then be available through
	// the Result method. It returns false if no further advancement is possible, or if an
	// error was encountered during iteration.  Err should be consulted to distinguish
	// between the two cases.
	Next(ctx context.Context) bool
	// Results returns the current result. The type depends on the collation mode of the query.
	Result() interface{}
	// Err returns any error that was encountered by the Iterator.
	Err() error
	// Close the iterator and do internal cleanup.
	Close() error
}

// Collation of results.
type Collation int

const (
	// Raw collates results as maps or arrays of graph.Refs or any other query-native or graph-native data type.
	Raw = Collation(iota)
	// REPL collates results as strings which will be used in CLI.
	REPL = Collation(iota)
	// JSON collates results as maps, arrays and values, that can be encoded to JSON.
	JSON
	// JSONLD collates results as maps, arrays and values compatible with JSON-LD spec.
	JSONLD
)

// Options for the query execution.
type Options struct {
	Limit     int
	Collation Collation
}

type Session interface {
	// Execute runs the query and returns an iterator over the results.
	// Type of results depends on Collation. See Options for details.
	Execute(ctx context.Context, query string, opt Options) (Iterator, error)
}

// TODO(dennwc): specify exact type to return from ShapeOf
// TODO(dennwc): add context to ShapeOf?

type HTTP interface {
	Session
	ShapeOf(string) (interface{}, error)
}

type REPLSession = Session

// ResponseWriter is a subset of http.ResponseWriter
type ResponseWriter interface {
	Write([]byte) (int, error)
	WriteHeader(int)
}

// Language is a description of query language.
type Language struct {
	Name    string
	Session func(graph.QuadStore) Session
	REPL    func(graph.QuadStore) REPLSession // deprecated
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

// Execute runs the query in a given language and returns an iterator over the results.
// Type of results depends on Collation. See Options for details.
func Execute(ctx context.Context, qs graph.QuadStore, lang, query string, opt Options) (Iterator, error) {
	l := GetLanguage(lang)
	if l == nil {
		return nil, fmt.Errorf("unsupported language: %q", lang)
	}
	sess := l.Session(qs)
	return sess.Execute(ctx, query, opt)
}
