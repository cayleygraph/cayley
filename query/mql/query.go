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

package mql

import (
	"fmt"
	"strings"

	"github.com/google/cayley/graph"
)

type (
	Path       string
	ResultPath string
)

type Query struct {
	ses            *Session
	it             graph.Iterator
	isRepeated     map[Path]bool
	queryStructure map[Path]map[string]interface{}
	queryResult    map[ResultPath]map[string]interface{}
	results        []interface{}
	resultOrder    []string
	err            error
}

func (q *Query) isError() bool {
	return q.err != nil
}

func (q *Query) copyPathStructure(path Path) map[string]interface{} {
	output := make(map[string]interface{})
	for k, v := range q.queryStructure[path] {
		output[k] = v
	}
	return output
}

func NewPath() Path {
	return ""
}
func (p Path) Follow(s string) Path {
	return Path(fmt.Sprintf("%s\x1E%s", p, s))
}

func (p Path) DisplayString() string {
	return strings.Replace(string(p), "\x1E", ".", -1)
}

func NewResultPath() ResultPath {
	return ""
}

func (p ResultPath) FollowPath(followPiece string, value string) ResultPath {
	if string(p) == "" {
		return ResultPath(fmt.Sprintf("%s\x1E%s", value, followPiece))
	}
	return ResultPath(fmt.Sprintf("%s\x1E%s\x1E%s", p, value, followPiece))
}

func (p ResultPath) getPath() Path {
	out := NewPath()
	pathPieces := strings.Split(string(p), "\x1E")
	for len(pathPieces) > 1 {
		a := pathPieces[1]
		pathPieces = pathPieces[2:]
		out = out.Follow(a)
	}
	return out
}

func (p ResultPath) splitLastPath() (ResultPath, string) {
	pathPieces := strings.Split(string(p), "\x1E")
	return ResultPath(strings.Join(pathPieces[:len(pathPieces)-1], "\x1E")), pathPieces[len(pathPieces)-1]
}

func (p ResultPath) AppendValue(value string) ResultPath {
	if string(p) == "" {
		return ResultPath(value)
	}
	return ResultPath(fmt.Sprintf("%s\x1E%s", p, value))
}

func (p Path) ToResultPathFromMap(resultMap map[Path]string) ResultPath {
	output := NewResultPath()
	pathPieces := strings.Split(string(p), "\x1E")[1:]
	pathSoFar := NewPath()
	for _, piece := range pathPieces {
		output = output.FollowPath(piece, resultMap[pathSoFar])
		pathSoFar = pathSoFar.Follow(piece)
	}
	return output
}

func NewQuery(ses *Session) *Query {
	var q Query
	q.ses = ses
	q.results = make([]interface{}, 0)
	q.resultOrder = make([]string, 0)
	q.err = nil
	return &q
}
