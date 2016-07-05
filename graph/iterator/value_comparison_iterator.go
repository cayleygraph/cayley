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
	"strconv"

	"github.com/cayleygraph/cayley/graph"
)

type Operator int

const (
	compareLT Operator = iota
	compareLTE
	compareGT
	compareGTE
	// Why no Equals? Because that's usually an AndIterator.
)

type Comparison struct {
	uid    uint64
	tags   graph.Tagger
	subIt  graph.Iterator
	op     Operator
	val    interface{}
	qs     graph.QuadStore
	result graph.Value
	err    error
}

func NewComparison(sub graph.Iterator, op Operator, val interface{}, qs graph.QuadStore) *Comparison {
	return &Comparison{
		uid:   NextUID(),
		subIt: sub,
		op:    op,
		val:   val,
		qs:    qs,
	}
}

func (it *Comparison) UID() uint64 {
	return it.uid
}

// Here's the non-boilerplate part of the ValueComparison iterator. Given a value
// and our operator, determine whether or not we meet the requirement.
func (it *Comparison) doComparison(val graph.Value) bool {
	nodeStr := it.qs.NameOf(val)
	switch cVal := it.val.(type) {
	case int:
		cInt := int64(cVal)
		intVal, err := strconv.ParseInt(nodeStr, 10, 64)
		if err != nil {
			return false
		}
		return RunIntOp(intVal, it.op, cInt)
	case int64:
		intVal, err := strconv.ParseInt(nodeStr, 10, 64)
		if err != nil {
			return false
		}
		return RunIntOp(intVal, it.op, cVal)
	case string:
		return RunStrOp(nodeStr, it.op, cVal)
	default:
		return true
	}
}

func (it *Comparison) Close() error {
	return it.subIt.Close()
}

func RunIntOp(a int64, op Operator, b int64) bool {
	switch op {
	case compareLT:
		return a < b
	case compareLTE:
		return a <= b
	case compareGT:
		return a > b
	case compareGTE:
		return a >= b
	default:
		panic("Unknown operator type")
	}
}

func RunStrOp(a string, op Operator, b string) bool {
	switch op {
	case compareLT:
		return a < b
	case compareLTE:
		return a <= b
	case compareGT:
		return a > b
	case compareGTE:
		return a >= b
	default:
		panic("Unknown operator type")
	}
}

func (it *Comparison) Reset() {
	it.subIt.Reset()
}

func (it *Comparison) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *Comparison) Clone() graph.Iterator {
	out := NewComparison(it.subIt.Clone(), it.op, it.val, it.qs)
	out.tags.CopyFrom(it)
	return out
}

func (it *Comparison) Next() bool {
	for graph.Next(it.subIt) {
		val := it.subIt.Result()
		if it.doComparison(val) {
			it.result = val
			return true
		}
	}
	it.err = it.subIt.Err()
	return false
}

func (it *Comparison) Err() error {
	return it.err
}

func (it *Comparison) Result() graph.Value {
	return it.result
}

func (it *Comparison) NextPath() bool {
	for {
		hasNext := it.subIt.NextPath()
		if !hasNext {
			it.err = it.subIt.Err()
			return false
		}
		if it.doComparison(it.subIt.Result()) {
			break
		}
	}
	it.result = it.subIt.Result()
	return true
}

// No subiterators.
func (it *Comparison) SubIterators() []graph.Iterator {
	return nil
}

func (it *Comparison) Contains(val graph.Value) bool {
	if !it.doComparison(val) {
		return false
	}
	ok := it.subIt.Contains(val)
	if !ok {
		it.err = it.subIt.Err()
	}
	return ok
}

// If we failed the check, then the subiterator should not contribute to the result
// set. Otherwise, go ahead and tag it.
func (it *Comparison) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}

	it.subIt.TagResults(dst)
}

// Registers the value-comparison iterator.
func (it *Comparison) Type() graph.Type { return graph.Comparison }

func (it *Comparison) Describe() graph.Description {
	primary := it.subIt.Describe()
	return graph.Description{
		UID:      it.UID(),
		Type:     it.Type(),
		Iterator: &primary,
	}
}

// There's nothing to optimize, locally, for a value-comparison iterator.
// Replace the underlying iterator if need be.
// potentially replace it.
func (it *Comparison) Optimize() (graph.Iterator, bool) {
	newSub, changed := it.subIt.Optimize()
	if changed {
		it.subIt.Close()
		it.subIt = newSub
	}
	return it, false
}

// We're only as expensive as our subiterator.
// Again, optimized value comparison iterators should do better.
func (it *Comparison) Stats() graph.IteratorStats {
	return it.subIt.Stats()
}

func (it *Comparison) Size() (int64, bool) {
	return 0, true
}

var _ graph.Nexter = &Comparison{}
