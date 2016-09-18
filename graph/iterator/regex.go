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

import (
	"regexp"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
)

// Regex is a unary operator -- a filter across the values in the relevant
// subiterator. It works similarly to gremlin's filter{it.matches('exp')},
// reducing the iterator set to values whose string representation passes a
// regular expression test.
type Regex struct {
	uid       uint64
	tags      graph.Tagger
	subIt     graph.Iterator
	re        *regexp.Regexp
	qs        graph.QuadStore
	result    graph.Value
	err       error
	allowRefs bool
}

func NewRegex(sub graph.Iterator, re *regexp.Regexp, qs graph.QuadStore) *Regex {
	return &Regex{
		uid:   NextUID(),
		subIt: sub,
		re:    re,
		qs:    qs,
	}
}

// AllowRefs allows regexp iterator to match IRIs and BNodes.
//
// Consider using it carefully. In most cases it's better to reconsider
// your graph structure instead of relying on slow unoptimizable regexp.
//
// An example of incorrect usage is to match IRIs:
// 	<http://example.org/page>
// 	<http://example.org/page/foo>
// Via regexp like:
//	http://example.org/page.*
//
// The right way is to explicitly link graph nodes and query them by this relation:
// 	<http://example.org/page/foo> <type> <http://example.org/page>
func (it *Regex) AllowRefs(v bool) {
	it.allowRefs = v
}

func (it *Regex) testRegex(val graph.Value) bool {
	// Type switch to avoid coercing and testing numeric types
	v := it.qs.NameOf(val)
	switch v := v.(type) {
	case quad.Raw:
		return it.re.MatchString(string(v))
	case quad.String:
		return it.re.MatchString(string(v))
	case quad.TypedString:
		return it.re.MatchString(string(v.Value))
	default:
		if it.allowRefs {
			switch v := v.(type) {
			case quad.BNode:
				return it.re.MatchString(string(v))
			case quad.IRI:
				return it.re.MatchString(string(v))
			}
		}
	}
	return false
}

func (it *Regex) UID() uint64 {
	return it.uid
}

func (it *Regex) Close() error {
	return it.subIt.Close()
}

func (it *Regex) Reset() {
	it.subIt.Reset()
	it.err = nil
	it.result = nil
}

func (it *Regex) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *Regex) Clone() graph.Iterator {
	out := NewRegex(it.subIt.Clone(), it.re, it.qs)
	out.tags.CopyFrom(it)
	return out
}

func (it *Regex) Next() bool {
	for it.subIt.Next() {
		val := it.subIt.Result()
		if it.testRegex(val) {
			it.result = val
			return true
		}
	}
	it.err = it.subIt.Err()
	return false
}

func (it *Regex) Err() error {
	return it.err
}

func (it *Regex) Result() graph.Value {
	return it.result
}

func (it *Regex) NextPath() bool {
	for {
		hasNext := it.subIt.NextPath()
		if !hasNext {
			it.err = it.subIt.Err()
			return false
		}
		if it.testRegex(it.subIt.Result()) {
			break
		}
	}
	it.result = it.subIt.Result()
	return true
}

func (it *Regex) SubIterators() []graph.Iterator {
	return []graph.Iterator{it.subIt}
}

func (it *Regex) Contains(val graph.Value) bool {
	if !it.testRegex(val) {
		return false
	}
	ok := it.subIt.Contains(val)
	if !ok {
		it.err = it.subIt.Err()
	}
	return ok
}

// Registers the Regex iterator.
func (it *Regex) Type() graph.Type {
	return graph.Regex
}

func (it *Regex) Describe() graph.Description {
	primary := it.subIt.Describe()
	return graph.Description{
		UID:      it.UID(),
		Type:     it.Type(),
		Iterator: &primary,
	}
}

// There's nothing to optimize, locally, for a Regex iterator.
// Replace the underlying iterator if need be.
func (it *Regex) Optimize() (graph.Iterator, bool) {
	newSub, changed := it.subIt.Optimize()
	if changed {
		it.subIt.Close()
		it.subIt = newSub
	}
	return it, false
}

// We're only as expensive as our subiterator.
func (it *Regex) Stats() graph.IteratorStats {
	return it.subIt.Stats()
}

// If we failed the check, then the subiterator should not contribute to the result
// set. Otherwise, go ahead and tag it.
func (it *Regex) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}

	it.subIt.TagResults(dst)
}

func (it *Regex) Size() (int64, bool) {
	return 0, false
}

var _ graph.Iterator = &Regex{}
