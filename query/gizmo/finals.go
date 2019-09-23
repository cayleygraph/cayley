// Copyright 2017 The Cayley Authors. All rights reserved.
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

package gizmo

import (
	"github.com/dop251/goja"

	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/quad"
)

const TopResultTag = "id"

// GetLimit is the same as All, but limited to the first N unique nodes at the end of the path, and each of their possible traversals.
func (p *pathObject) GetLimit(limit int) error {
	it := p.buildIteratorTree()
	it = iterator.Tag(it, TopResultTag)
	p.s.limit = limit
	p.s.count = 0
	return p.s.runIterator(it)
}

// All executes the query and adds the results, with all tags, as a string-to-string (tag to node) map in the output set, one for each path that a traversal could take.
func (p *pathObject) All() error {
	return p.GetLimit(p.s.limit)
}

func (p *pathObject) toArray(call goja.FunctionCall, withTags bool) goja.Value {
	args := exportArgs(call.Arguments)
	if len(args) > 1 {
		return throwErr(p.s.vm, errArgCount2{Expected: 1, Got: len(args)})
	}
	limit := -1
	if len(args) > 0 {
		limit, _ = toInt(args[0])
	}
	it := p.buildIteratorTree()
	it = iterator.Tag(it, TopResultTag)
	var (
		array interface{}
		err   error
	)
	if !withTags {
		array, err = p.s.runIteratorToArrayNoTags(it, limit)
	} else {
		array, err = p.s.runIteratorToArray(it, limit)
	}
	if err != nil {
		return throwErr(p.s.vm, err)
	}
	return p.s.vm.ToValue(array)
}

// ToArray executes a query and returns the results at the end of the query path as an JS array.
//
// Example:
// 	// javascript
//	// bobFollowers contains an Array of followers of bob (alice, charlie, dani).
//	var bobFollowers = g.V("<bob>").In("<follows>").ToArray()
func (p *pathObject) ToArray(call goja.FunctionCall) goja.Value {
	return p.toArray(call, false)
}

// TagArray is the same as ToArray, but instead of a list of top-level nodes, returns an Array of tag-to-string dictionaries, much as All would, except inside the JS environment.
//
// Example:
// 	// javascript
//	// bobTags contains an Array of followers of bob (alice, charlie, dani).
//	var bobTags = g.V("<bob>").Tag("name").In("<follows>").TagArray()
//	// nameValue should be the string "<bob>"
//	var nameValue = bobTags[0]["name"]
func (p *pathObject) TagArray(call goja.FunctionCall) goja.Value {
	return p.toArray(call, true)
}
func (p *pathObject) toValue(withTags bool) (interface{}, error) {
	it := p.buildIteratorTree()
	it = iterator.Tag(it, TopResultTag)
	const limit = 1
	if !withTags {
		array, err := p.s.runIteratorToArrayNoTags(it, limit)
		if err != nil {
			return nil, err
		}
		if len(array) == 0 {
			return nil, nil
		}
		return array[0], nil
	} else {
		array, err := p.s.runIteratorToArray(it, limit)
		if err != nil {
			return nil, err
		}
		if len(array) == 0 {
			return nil, nil
		}
		return array[0], nil
	}
}

// ToValue is the same as ToArray, but limited to one result node.
func (p *pathObject) ToValue() (interface{}, error) {
	return p.toValue(false)
}

// TagValue is the same as TagArray, but limited to one result node. Returns a tag-to-string map.
func (p *pathObject) TagValue() (interface{}, error) {
	return p.toValue(true)
}

// Map is a alias for ForEach.
func (p *pathObject) Map(call goja.FunctionCall) goja.Value {
	return p.ForEach(call)
}

// ForEach calls callback(data) for each result, where data is the tag-to-string map as in All case.
// Signature: (callback) or (limit, callback)
//
// Arguments:
//
// * `limit` (Optional): An integer value on the first `limit` paths to process.
// * `callback`: A javascript function of the form `function(data)`
//
// Example:
// 	// javascript
//	// Simulate query.All().All()
//	graph.V("<alice>").ForEach(function(d) { g.Emit(d) } )
func (p *pathObject) ForEach(call goja.FunctionCall) goja.Value {
	it := p.buildIteratorTree()
	it = iterator.Tag(it, TopResultTag)
	if n := len(call.Arguments); n != 1 && n != 2 {
		return throwErr(p.s.vm, errArgCount{Got: len(call.Arguments)})
	}
	callback := call.Argument(len(call.Arguments) - 1)
	args := exportArgs(call.Arguments[:len(call.Arguments)-1])
	limit := -1
	if len(args) != 0 {
		limit, _ = toInt(args[0])
	}
	err := p.s.runIteratorWithCallback(it, callback, call, limit)
	if err != nil {
		return throwErr(p.s.vm, err)
	}
	return goja.Null()
}

// Count returns a number of results and returns it as a value.
//
// Example:
//	// javascript
//	// Save count as a variable
//	var n = g.V().count()
//	// Send it as a query result
//	g.emit(n)
func (p *pathObject) Count() (int64, error) {
	it := p.buildIteratorTree()
	return p.s.countResults(it)
}

// Backwards compatibility
func (p *pathObject) CapitalizedGetLimit(limit int) error {
	return p.GetLimit(limit)
}
func (p *pathObject) CapitalizedAll() error {
	return p.All()
}
func (p *pathObject) CapitalizedtoArray(call goja.FunctionCall, withTags bool) goja.Value {
	return p.toArray(call, withTags)
}
func (p *pathObject) CapitalizedToArray(call goja.FunctionCall) goja.Value {
	return p.ToArray(call)
}
func (p *pathObject) CapitalizedTagArray(call goja.FunctionCall) goja.Value {
	return p.TagArray(call)
}
func (p *pathObject) CapitalizedtoValue(withTags bool) (interface{}, error) {
	return p.toValue(withTags)
}
func (p *pathObject) CapitalizedToValue() (interface{}, error) {
	return p.ToValue()
}
func (p *pathObject) CapitalizedTagValue() (interface{}, error) {
	return p.TagValue()
}
func (p *pathObject) CapitalizedMap(call goja.FunctionCall) goja.Value {
	return p.Map(call)
}
func (p *pathObject) CapitalizedForEach(call goja.FunctionCall) goja.Value {
	return p.ForEach(call)
}
func (p *pathObject) CapitalizedCount() (int64, error) {
	return p.Count()
}

func quadValueToString(v quad.Value) string {
	if s, ok := v.(quad.String); ok {
		return string(s)
	}
	return quad.StringOf(v)
}
