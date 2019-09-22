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

package iterator

// "Value Comparison" is a unary operator -- a filter across the values in the
// relevant subiterator.
//
// This is hugely useful for things like label, but value ranges in general
// come up from time to time. At *worst* we're as big as our underlying iterator.
// At best, we're the null iterator.
//
// This is ripe for backend-side optimization. If you can run a value iterator,
// from a sorted set -- some sort of value index, then go for it.
//
// In MQL terms, this is the [{"age>=": 21}] concept.

import (
	"fmt"
	"time"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"
)

type Operator int

func (op Operator) String() string {
	switch op {
	case CompareLT:
		return "<"
	case CompareLTE:
		return "<="
	case CompareGT:
		return ">"
	case CompareGTE:
		return ">="
	default:
		return fmt.Sprintf("op(%d)", int(op))
	}
}

const (
	CompareLT Operator = iota
	CompareLTE
	CompareGT
	CompareGTE
	// Why no Equals? Because that's usually an AndIterator.
)

func NewComparison(sub graph.Iterator, op Operator, val quad.Value, qs graph.Namer) graph.Iterator {
	return NewValueFilter(qs, sub, func(qval quad.Value) (bool, error) {
		switch cVal := val.(type) {
		case quad.Int:
			if cVal2, ok := qval.(quad.Int); ok {
				return RunIntOp(cVal2, op, cVal), nil
			}
			return false, nil
		case quad.Float:
			if cVal2, ok := qval.(quad.Float); ok {
				return RunFloatOp(cVal2, op, cVal), nil
			}
			return false, nil
		case quad.String:
			if cVal2, ok := qval.(quad.String); ok {
				return RunStrOp(string(cVal2), op, string(cVal)), nil
			}
			return false, nil
		case quad.BNode:
			if cVal2, ok := qval.(quad.BNode); ok {
				return RunStrOp(string(cVal2), op, string(cVal)), nil
			}
			return false, nil
		case quad.IRI:
			if cVal2, ok := qval.(quad.IRI); ok {
				return RunStrOp(string(cVal2), op, string(cVal)), nil
			}
			return false, nil
		case quad.Time:
			if cVal2, ok := qval.(quad.Time); ok {
				return RunTimeOp(time.Time(cVal2), op, time.Time(cVal)), nil
			}
			return false, nil
		default:
			return RunStrOp(quad.StringOf(qval), op, quad.StringOf(val)), nil
		}
	})
}

func RunIntOp(a quad.Int, op Operator, b quad.Int) bool {
	switch op {
	case CompareLT:
		return a < b
	case CompareLTE:
		return a <= b
	case CompareGT:
		return a > b
	case CompareGTE:
		return a >= b
	default:
		panic("Unknown operator type")
	}
}

func RunFloatOp(a quad.Float, op Operator, b quad.Float) bool {
	switch op {
	case CompareLT:
		return a < b
	case CompareLTE:
		return a <= b
	case CompareGT:
		return a > b
	case CompareGTE:
		return a >= b
	default:
		panic("Unknown operator type")
	}
}

func RunStrOp(a string, op Operator, b string) bool {
	switch op {
	case CompareLT:
		return a < b
	case CompareLTE:
		return a <= b
	case CompareGT:
		return a > b
	case CompareGTE:
		return a >= b
	default:
		panic("Unknown operator type")
	}
}

func RunTimeOp(a time.Time, op Operator, b time.Time) bool {
	switch op {
	case CompareLT:
		return a.Before(b)
	case CompareLTE:
		return !a.After(b)
	case CompareGT:
		return a.After(b)
	case CompareGTE:
		return !a.Before(b)
	default:
		panic("Unknown operator type")
	}
}
