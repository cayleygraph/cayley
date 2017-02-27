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

	"github.com/codelingo/cayley/quad"
)

const TopResultTag = "id"

func (p *pathObject) GetLimit(limit int) error {
	it := p.buildIteratorTree()
	it.Tagger().Add(TopResultTag)
	p.s.limit = limit
	p.s.count = 0
	return p.s.runIterator(it)
}

func (p *pathObject) All() error {
	return p.GetLimit(-1)
}

func (p *pathObject) toArray(call goja.FunctionCall, withTags bool) goja.Value {
	args := exportArgs(call.Arguments)
	if len(args) > 1 {
		return throwErr(p.s.vm, errArgCount2{Expected: 1, Got: len(args)})
	}
	limit := -1
	if len(args) > 0 {
		limit = toInt(args[0])
	}
	it := p.buildIteratorTree()
	it.Tagger().Add(TopResultTag)
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
func (p *pathObject) ToArray(call goja.FunctionCall) goja.Value {
	return p.toArray(call, false)
}
func (p *pathObject) TagArray(call goja.FunctionCall) goja.Value {
	return p.toArray(call, true)
}
func (p *pathObject) toValue(withTags bool) (interface{}, error) {
	it := p.buildIteratorTree()
	it.Tagger().Add(TopResultTag)
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
func (p *pathObject) ToValue() (interface{}, error) {
	return p.toValue(false)
}
func (p *pathObject) TagValue() (interface{}, error) {
	return p.toValue(true)
}
func (p *pathObject) Map(call goja.FunctionCall) goja.Value {
	return p.ForEach(call)
}
func (p *pathObject) ForEach(call goja.FunctionCall) goja.Value {
	it := p.buildIteratorTree()
	it.Tagger().Add(TopResultTag)
	if n := len(call.Arguments); n != 1 && n != 2 {
		return throwErr(p.s.vm, errArgCount{Got: len(call.Arguments)})
	}
	callback := call.Argument(len(call.Arguments) - 1)
	args := exportArgs(call.Arguments[:len(call.Arguments)-1])
	limit := -1
	if len(args) != 0 {
		limit = toInt(args[0])
	}
	err := p.s.runIteratorWithCallback(it, callback, call, limit)
	if err != nil {
		return throwErr(p.s.vm, err)
	}
	return goja.Null()
}

func quadValueToString(v quad.Value) string {
	if s, ok := v.(quad.String); ok {
		return string(s)
	}
	return quad.StringOf(v)
}

func quadValueToNative(v quad.Value) interface{} {
	out := v.Native()
	if nv, ok := out.(quad.Value); ok && v == nv {
		return quad.StringOf(v)
	}
	return out
}
