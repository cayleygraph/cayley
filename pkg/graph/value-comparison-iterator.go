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

package graph

// "Value Comparison" is a unary operator -- a filter across the values in the
// relevant subiterator.
//
// This is hugely useful for things like provenance, but value ranges in general
// come up from time to time. At *worst* we're as big as our underlying iterator.
// At best, we're the null iterator.
//
// This is ripe for backend-side optimization. If you can run a value iterator,
// from a sorted set -- some sort of value index, then go for it.
//
// In MQL terms, this is the [{"age>=": 21}] concept.

import (
	"fmt"
	"log"
	"strconv"
	"strings"
)

type ComparisonOperator int

const (
	kCompareLT ComparisonOperator = iota
	kCompareLTE
	kCompareGT
	kCompareGTE
	// Why no Equals? Because that's usually an AndIterator.
)

type ValueComparisonIterator struct {
	BaseIterator
	subIt           Iterator
	op              ComparisonOperator
	comparisonValue interface{}
	ts              TripleStore
}

func NewValueComparisonIterator(
	subIt Iterator,
	operator ComparisonOperator,
	value interface{},
	ts TripleStore) *ValueComparisonIterator {

	var vc ValueComparisonIterator
	BaseIteratorInit(&vc.BaseIterator)
	vc.subIt = subIt
	vc.op = operator
	vc.comparisonValue = value
	vc.ts = ts
	return &vc
}

// Here's the non-boilerplate part of the ValueComparison iterator. Given a value
// and our operator, determine whether or not we meet the requirement.
func (vc *ValueComparisonIterator) doComparison(val TSVal) bool {
	//TODO(barakmich): Implement string comparison.
	nodeStr := vc.ts.GetNameFor(val)
	switch cVal := vc.comparisonValue.(type) {
	case int:
		cInt := int64(cVal)
		intVal, err := strconv.ParseInt(nodeStr, 10, 64)
		if err != nil {
			return false
		}
		return RunIntOp(intVal, vc.op, cInt)
	case int64:
		intVal, err := strconv.ParseInt(nodeStr, 10, 64)
		if err != nil {
			return false
		}
		return RunIntOp(intVal, vc.op, cVal)
	default:
		return true
	}
}

func (vc *ValueComparisonIterator) Close() {
	vc.subIt.Close()
}

func RunIntOp(a int64, op ComparisonOperator, b int64) bool {
	switch op {
	case kCompareLT:
		return a < b
	case kCompareLTE:
		return a <= b
	case kCompareGT:
		return a > b
	case kCompareGTE:
		return a >= b
	default:
		log.Fatal("Unknown operator type")
		return false
	}
}

func (vc *ValueComparisonIterator) Reset() {
	vc.subIt.Reset()
}

func (vc *ValueComparisonIterator) Clone() Iterator {
	out := NewValueComparisonIterator(vc.subIt.Clone(), vc.op, vc.comparisonValue, vc.ts)
	out.CopyTagsFrom(vc)
	return out
}

func (vc *ValueComparisonIterator) Next() (TSVal, bool) {
	var val TSVal
	var ok bool
	for {
		val, ok = vc.subIt.Next()
		if !ok {
			return nil, false
		}
		if vc.doComparison(val) {
			break
		}
	}
	vc.Last = val
	return val, ok
}

func (vc *ValueComparisonIterator) NextResult() bool {
	for {
		hasNext := vc.subIt.NextResult()
		if !hasNext {
			return false
		}
		if vc.doComparison(vc.subIt.LastResult()) {
			return true
		}
	}
	vc.Last = vc.subIt.LastResult()
	return true
}

func (vc *ValueComparisonIterator) Check(val TSVal) bool {
	if !vc.doComparison(val) {
		return false
	}
	return vc.subIt.Check(val)
}

// If we failed the check, then the subiterator should not contribute to the result
// set. Otherwise, go ahead and tag it.
func (vc *ValueComparisonIterator) TagResults(out *map[string]TSVal) {
	vc.BaseIterator.TagResults(out)
	vc.subIt.TagResults(out)
}

// Registers the value-comparison iterator.
func (vc *ValueComparisonIterator) Type() string { return "value-comparison" }

// Prints the value-comparison and its subiterator.
func (vc *ValueComparisonIterator) DebugString(indent int) string {
	return fmt.Sprintf("%s(%s\n%s)",
		strings.Repeat(" ", indent),
		vc.Type(), vc.subIt.DebugString(indent+4))
}

// There's nothing to optimize, locally, for a value-comparison iterator.
// Replace the underlying iterator if need be.
// potentially replace it.
func (vc *ValueComparisonIterator) Optimize() (Iterator, bool) {
	newSub, changed := vc.subIt.Optimize()
	if changed {
		vc.subIt.Close()
		vc.subIt = newSub
	}
	return vc, false
}

// We're only as expensive as our subiterator.
// Again, optimized value comparison iterators should do better.
func (vc *ValueComparisonIterator) GetStats() *IteratorStats {
	return vc.subIt.GetStats()
}
