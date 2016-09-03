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
	"errors"
	"github.com/cayleygraph/cayley/graph"
	"golang.org/x/net/context"
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
