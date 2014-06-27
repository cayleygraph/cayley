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
	"github.com/google/cayley/src/graph"
	"strings"
)

type MqlPath string
type MqlResultPath string

type MqlQuery struct {
	ses            *MqlSession
	it             graph.Iterator
	isRepeated     map[MqlPath]bool
	queryStructure map[MqlPath]map[string]interface{}
	queryResult    map[MqlResultPath]map[string]interface{}
	results        []interface{}
	resultOrder    []string
	isError        bool
	err            error
}

func (mqlQuery *MqlQuery) copyPathStructure(path MqlPath) map[string]interface{} {
	output := make(map[string]interface{})
	for k, v := range mqlQuery.queryStructure[path] {
		output[k] = v
	}
	return output
}

func NewMqlPath() MqlPath {
	return ""
}
func (p MqlPath) Follow(s string) MqlPath {
	return MqlPath(fmt.Sprintf("%s\x1E%s", p, s))
}

func (p MqlPath) DisplayString() string {
	return strings.Replace(string(p), "\x1E", ".", -1)
}

func NewMqlResultPath() MqlResultPath {
	return ""
}

func (p MqlResultPath) FollowPath(followPiece string, value string) MqlResultPath {
	if string(p) == "" {
		return MqlResultPath(fmt.Sprintf("%s\x1E%s", value, followPiece))
	}
	return MqlResultPath(fmt.Sprintf("%s\x1E%s\x1E%s", p, value, followPiece))
}

func (p MqlResultPath) getPath() MqlPath {
	out := NewMqlPath()
	pathPieces := strings.Split(string(p), "\x1E")
	for len(pathPieces) > 1 {
		a := pathPieces[1]
		pathPieces = pathPieces[2:]
		out = out.Follow(a)
	}
	return out
}

func (p MqlResultPath) splitLastPath() (MqlResultPath, string) {
	pathPieces := strings.Split(string(p), "\x1E")
	return MqlResultPath(strings.Join(pathPieces[:len(pathPieces)-1], "\x1E")), pathPieces[len(pathPieces)-1]
}

func (p MqlResultPath) AppendValue(value string) MqlResultPath {
	if string(p) == "" {
		return MqlResultPath(value)
	}
	return MqlResultPath(fmt.Sprintf("%s\x1E%s", p, value))
}

func (p MqlPath) ToResultPathFromMap(resultMap map[MqlPath]string) MqlResultPath {
	output := NewMqlResultPath()
	pathPieces := strings.Split(string(p), "\x1E")[1:]
	pathSoFar := NewMqlPath()
	for _, piece := range pathPieces {
		output = output.FollowPath(piece, resultMap[pathSoFar])
		pathSoFar = pathSoFar.Follow(piece)
	}
	return output
}

func NewMqlQuery(ses *MqlSession) *MqlQuery {
	var q MqlQuery
	q.ses = ses
	q.results = make([]interface{}, 0)
	q.resultOrder = make([]string, 0)
	q.err = nil
	q.isError = false
	return &q
}
