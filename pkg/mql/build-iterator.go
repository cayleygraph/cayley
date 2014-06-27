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
	"github.com/google/cayley/pkg/graph"
	"log"
	"math"
	"strings"
)

func (m *MqlQuery) buildFixed(s string) graph.Iterator {
	f := m.ses.ts.MakeFixed()
	f.AddValue(m.ses.ts.GetIdFor(s))
	return f
}

func (m *MqlQuery) buildResultIterator(path MqlPath) graph.Iterator {
	all := m.ses.ts.GetNodesAllIterator()
	all.AddTag(string(path))
	return graph.NewOptionalIterator(all)
}

func (m *MqlQuery) BuildIteratorTree(query interface{}) {
	m.isRepeated = make(map[MqlPath]bool)
	m.queryStructure = make(map[MqlPath]map[string]interface{})
	m.queryResult = make(map[MqlResultPath]map[string]interface{})
	m.queryResult[""] = make(map[string]interface{})

	m.it, m.err = m.buildIteratorTreeInternal(query, NewMqlPath())
	if m.err != nil {
		m.isError = true
	}
}

func (m *MqlQuery) buildIteratorTreeInternal(query interface{}, path MqlPath) (graph.Iterator, error) {
	var it graph.Iterator
	var err error
	err = nil
	switch t := query.(type) {
	case bool:
		// for JSON booleans
		// Treat the bool as a string and call it a day.
		// Things which are really bool-like are special cases and will be dealt with separately.
		if t {
			it = m.buildFixed("true")
		}
		it = m.buildFixed("false")
	case float64:
		// for JSON numbers
		// Damn you, Javascript, and your lack of integer values.
		if math.Floor(t) == t {
			// Treat it like an integer.
			it = m.buildFixed(fmt.Sprintf("%d", t))
		} else {
			it = m.buildFixed(fmt.Sprintf("%f", t))
		}
	case string:
		// for JSON strings
		it = m.buildFixed(t)
	case []interface{}:
		// for JSON arrays
		m.isRepeated[path] = true
		if len(t) == 0 {
			it = m.buildResultIterator(path)
		} else if len(t) == 1 {
			it, err = m.buildIteratorTreeInternal(t[0], path)
		} else {
			err = errors.New(fmt.Sprintf("Multiple fields at location root%s", path.DisplayString()))
		}
	case map[string]interface{}:
		// for JSON objects
		it, err = m.buildIteratorTreeMapInternal(t, path)
	case nil:
		it = m.buildResultIterator(path)
	default:
		log.Fatal("Unknown JSON type?", query)
	}
	if err != nil {
		return nil, err
	}
	it.AddTag(string(path))
	return it, nil
}

func (m *MqlQuery) buildIteratorTreeMapInternal(query map[string]interface{}, path MqlPath) (graph.Iterator, error) {
	it := graph.NewAndIterator()
	it.AddSubIterator(m.ses.ts.GetNodesAllIterator())
	var err error
	err = nil
	outputStructure := make(map[string]interface{})
	for key, subquery := range query {
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
			subit, err = m.buildIteratorTreeInternal(subquery, path.Follow(key))
			if err != nil {
				return nil, err
			}
			it.AddSubIterator(subit)
		} else {
			subit, err = m.buildIteratorTreeInternal(subquery, path.Follow(key))
			if err != nil {
				return nil, err
			}
			subAnd := graph.NewAndIterator()
			predFixed := m.ses.ts.MakeFixed()
			predFixed.AddValue(m.ses.ts.GetIdFor(pred))
			subAnd.AddSubIterator(graph.NewLinksToIterator(m.ses.ts, predFixed, "p"))
			if reverse {
				lto := graph.NewLinksToIterator(m.ses.ts, subit, "s")
				subAnd.AddSubIterator(lto)
				hasa := graph.NewHasaIterator(m.ses.ts, subAnd, "o")
				it.AddSubIterator(hasa)
			} else {
				lto := graph.NewLinksToIterator(m.ses.ts, subit, "o")
				subAnd.AddSubIterator(lto)
				hasa := graph.NewHasaIterator(m.ses.ts, subAnd, "s")
				it.AddSubIterator(hasa)
			}
		}
	}
	if err != nil {
		return nil, err
	}
	m.queryStructure[path] = outputStructure
	return it, nil
}

type MqlResultPathSlice []MqlResultPath

func (sl MqlResultPathSlice) Len() int {
	return len(sl)
}

func (sl MqlResultPathSlice) Less(i, j int) bool {
	iLen := len(strings.Split(string(sl[i]), "\x30"))
	jLen := len(strings.Split(string(sl[j]), "\x30"))
	if iLen < jLen {
		return true
	}
	if iLen == jLen {
		if len(string(sl[i])) < len(string(sl[j])) {
			return true
		}
	}
	return false
}

func (sl MqlResultPathSlice) Swap(i, j int) {
	sl[i], sl[j] = sl[j], sl[i]
}
