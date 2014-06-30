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
	"errors"
	"fmt"
	"log"
	"math"
	"strings"

	"github.com/google/cayley/graph"
)

func (q *Query) buildFixed(s string) graph.Iterator {
	f := q.ses.ts.MakeFixed()
	f.AddValue(q.ses.ts.GetIdFor(s))
	return f
}

func (q *Query) buildResultIterator(path Path) graph.Iterator {
	all := q.ses.ts.GetNodesAllIterator()
	all.AddTag(string(path))
	return all
}

func (q *Query) BuildIteratorTree(query interface{}) {
	q.isRepeated = make(map[Path]bool)
	q.queryStructure = make(map[Path]map[string]interface{})
	q.queryResult = make(map[ResultPath]map[string]interface{})
	q.queryResult[""] = make(map[string]interface{})

	var isOptional bool
	q.it, isOptional, q.err = q.buildIteratorTreeInternal(query, NewPath())
	if isOptional {
		q.err = errors.New("Optional iterator at the top level?")
	}
}

func (q *Query) buildIteratorTreeInternal(query interface{}, path Path) (it graph.Iterator, optional bool, err error) {
	err = nil
	optional = false
	switch t := query.(type) {
	case bool:
		// for JSON booleans
		// Treat the bool as a string and call it a day.
		// Things which are really bool-like are special cases and will be dealt with separately.
		if t {
			it = q.buildFixed("true")
		}
		it = q.buildFixed("false")
	case float64:
		// for JSON numbers
		// Damn you, Javascript, and your lack of integer values.
		if math.Floor(t) == t {
			// Treat it like an integer.
			it = q.buildFixed(fmt.Sprintf("%d", t))
		} else {
			it = q.buildFixed(fmt.Sprintf("%f", t))
		}
	case string:
		// for JSON strings
		it = q.buildFixed(t)
	case []interface{}:
		// for JSON arrays
		q.isRepeated[path] = true
		if len(t) == 0 {
			it = q.buildResultIterator(path)
			optional = true
		} else if len(t) == 1 {
			it, optional, err = q.buildIteratorTreeInternal(t[0], path)
		} else {
			err = errors.New(fmt.Sprintf("Multiple fields at location root%s", path.DisplayString()))
		}
	case map[string]interface{}:
		// for JSON objects
		it, err = q.buildIteratorTreeMapInternal(t, path)
	case nil:
		it = q.buildResultIterator(path)
		optional = true
	default:
		log.Fatal("Unknown JSON type?", query)
	}
	if err != nil {
		return nil, false, err
	}
	it.AddTag(string(path))
	return it, optional, nil
}

func (q *Query) buildIteratorTreeMapInternal(query map[string]interface{}, path Path) (graph.Iterator, error) {
	it := graph.NewAndIterator()
	it.AddSubIterator(q.ses.ts.GetNodesAllIterator())
	var err error
	err = nil
	outputStructure := make(map[string]interface{})
	for key, subquery := range query {
		optional := false
		outputStructure[key] = nil
		reverse := false
		pred := key
		if strings.HasPrefix(pred, "@") {
			i := strings.Index(pred, ":")
			if i != -1 {
				pred = pred[(i + 1):]
			}
		}
		if strings.HasPrefix(pred, "!") {
			reverse = true
			pred = strings.TrimPrefix(pred, "!")
		}

		// Other special constructs here
		var subit graph.Iterator
		if key == "id" {
			subit, optional, err = q.buildIteratorTreeInternal(subquery, path.Follow(key))
			if err != nil {
				return nil, err
			}
		} else {
			var builtIt graph.Iterator
			builtIt, optional, err = q.buildIteratorTreeInternal(subquery, path.Follow(key))
			if err != nil {
				return nil, err
			}
			subAnd := graph.NewAndIterator()
			predFixed := q.ses.ts.MakeFixed()
			predFixed.AddValue(q.ses.ts.GetIdFor(pred))
			subAnd.AddSubIterator(graph.NewLinksToIterator(q.ses.ts, predFixed, "p"))
			if reverse {
				lto := graph.NewLinksToIterator(q.ses.ts, builtIt, "s")
				subAnd.AddSubIterator(lto)
				hasa := graph.NewHasaIterator(q.ses.ts, subAnd, "o")
				subit = hasa
			} else {
				lto := graph.NewLinksToIterator(q.ses.ts, builtIt, "o")
				subAnd.AddSubIterator(lto)
				hasa := graph.NewHasaIterator(q.ses.ts, subAnd, "s")
				subit = hasa
			}
		}
		if optional {
			it.AddSubIterator(graph.NewOptionalIterator(subit))
		} else {
			it.AddSubIterator(subit)
		}
	}
	if err != nil {
		return nil, err
	}
	q.queryStructure[path] = outputStructure
	return it, nil
}

type ResultPathSlice []ResultPath

func (p ResultPathSlice) Len() int {
	return len(p)
}

func (p ResultPathSlice) Less(i, j int) bool {
	iLen := len(strings.Split(string(p[i]), "\x30"))
	jLen := len(strings.Split(string(p[j]), "\x30"))
	if iLen < jLen {
		return true
	}
	if iLen == jLen {
		if len(string(p[i])) < len(string(p[j])) {
			return true
		}
	}
	return false
}

func (p ResultPathSlice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
