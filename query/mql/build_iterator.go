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
	"math"
	"strings"

	"github.com/cayleygraph/cayley/graph/shape"
	"github.com/cayleygraph/quad"
)

func buildFixed(s string) shape.Shape {
	return shape.Lookup{quad.StringToValue(s)}
}

func buildAllResult(path Path) shape.Shape {
	return shape.Save{
		From: shape.AllNodes{},
		Tags: []string{string(path)},
	}
}

func (q *Query) BuildIteratorTree(query interface{}) {
	q.isRepeated = make(map[Path]bool)
	q.queryStructure = make(map[Path]map[string]interface{})
	q.queryResult = make(map[ResultPath]map[string]interface{})
	q.queryResult[""] = make(map[string]interface{})

	var (
		opt bool
		s   shape.Shape
	)
	s, opt, q.err = q.buildShape(query, NewPath())
	if q.err == nil && opt {
		q.err = errors.New("optional iterator at the top level")
	}
	q.it = shape.BuildIterator(q.ses.qs, s)
}

func (q *Query) buildShape(query interface{}, path Path) (s shape.Shape, optional bool, err error) {
	err = nil
	optional = false
	switch t := query.(type) {
	case bool:
		// for JSON booleans
		s = shape.Lookup{quad.Bool(t)}
	case float64:
		// for JSON numbers
		// Damn you, Javascript, and your lack of integer values.
		if math.Floor(t) == t {
			// Treat it like an integer.
			s = shape.Lookup{quad.Int(t)}
		} else {
			s = shape.Lookup{quad.Float(t)}
		}
	case string:
		// for JSON strings
		s = buildFixed(t)
	case []interface{}:
		// for JSON arrays
		q.isRepeated[path] = true
		if len(t) == 0 {
			s = buildAllResult(path)
			optional = true
		} else if len(t) == 1 {
			s, optional, err = q.buildShape(t[0], path)
		} else {
			err = fmt.Errorf("multiple fields at location root %s", path.DisplayString())
		}
	case map[string]interface{}:
		// for JSON objects
		s, err = q.buildShapeMap(t, path)
	case nil:
		s = buildAllResult(path)
		optional = true
	default:
		err = fmt.Errorf("Unknown JSON type: %T", query)
	}
	if err != nil {
		return nil, false, err
	}
	s = shape.Save{
		From: s,
		Tags: []string{string(path)},
	}
	return s, optional, nil
}

func (q *Query) buildShapeMap(query map[string]interface{}, path Path) (shape.Shape, error) {
	it := shape.IntersectOpt{
		Sub: shape.Intersect{
			shape.AllNodes{},
		},
	}
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
		var subit shape.Shape
		if key == "id" {
			var err error
			subit, optional, err = q.buildShape(subquery, path.Follow(key))
			if err != nil {
				return nil, err
			}
		} else {
			var (
				builtIt shape.Shape
				err     error
			)
			builtIt, optional, err = q.buildShape(subquery, path.Follow(key))
			if err != nil {
				return nil, err
			}
			from, to := quad.Subject, quad.Object
			if reverse {
				from, to = to, from
			}
			subit = shape.NodesFrom{
				Dir: from,
				Quads: shape.Quads{
					{Dir: quad.Predicate, Values: buildFixed(pred)},
					{Dir: to, Values: builtIt},
				},
			}
		}
		if optional {
			it.AddOptional(subit)
		} else {
			it.Add(subit)
		}
	}
	q.queryStructure[path] = outputStructure
	if len(it.Opt) == 0 {
		return it.Sub, nil
	}
	return it, nil
}

type byRecordLength []ResultPath

func (p byRecordLength) Len() int {
	return len(p)
}

func (p byRecordLength) Less(i, j int) bool {
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

func (p byRecordLength) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
