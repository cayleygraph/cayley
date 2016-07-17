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

package gremlin

import (
	"encoding/json"

	"github.com/robertkrimen/otto"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
)

const TopResultTag = "id"

func (p *pathObject) getLimit(limit int) otto.Value {
	it := p.buildIteratorTree()
	it.Tagger().Add(TopResultTag)
	p.wk.limit = limit
	p.wk.count = 0
	p.wk.runIterator(it)
	return otto.NullValue()
}

func (p *pathObject) All(call otto.FunctionCall) otto.Value {
	return p.getLimit(-1)
}

func (p *pathObject) GetLimit(call otto.FunctionCall) otto.Value {
	args := exportArgs(call.ArgumentList)
	if len(args) != 1 {
		return otto.NullValue()
	}
	return p.getLimit(toInt(args[0]))
}
func (p *pathObject) toArray(call otto.FunctionCall, withTags bool) otto.Value {
	args := exportArgs(call.ArgumentList)
	if len(args) != 1 {
		return otto.NullValue()
	}
	limit := -1
	if len(args) > 0 {
		limit = toInt(args[0])
	}
	it := p.buildIteratorTree()
	it.Tagger().Add(TopResultTag)
	var (
		val otto.Value
		err error
	)
	if !withTags {
		array := p.wk.runIteratorToArrayNoTags(it, limit)
		val, err = call.Otto.ToValue(array)
	} else {
		array := p.wk.runIteratorToArray(it, limit)
		val, err = call.Otto.ToValue(array)
	}
	if err != nil {
		clog.Errorf("%v", err)
		return otto.NullValue()
	}
	return val
}
func (p *pathObject) ToArray(call otto.FunctionCall) otto.Value {
	return p.toArray(call, false)
}
func (p *pathObject) TagArray(call otto.FunctionCall) otto.Value {
	return p.toArray(call, true)
}
func (p *pathObject) toValue(call otto.FunctionCall, withTags bool) otto.Value {
	it := p.buildIteratorTree()
	it.Tagger().Add(TopResultTag)
	const limit = 1
	var (
		val otto.Value
		err error
	)
	if !withTags {
		array := p.wk.runIteratorToArrayNoTags(it, limit)
		if len(array) < 1 {
			return otto.NullValue()
		}
		val, err = call.Otto.ToValue(array[0])
	} else {
		array := p.wk.runIteratorToArray(it, limit)
		if len(array) < 1 {
			return otto.NullValue()
		}
		val, err = call.Otto.ToValue(array[0])
	}
	if err != nil {
		clog.Errorf("%v", err)
		return otto.NullValue()
	}
	return val
}
func (p *pathObject) ToValue(call otto.FunctionCall) otto.Value {
	return p.toValue(call, false)
}
func (p *pathObject) TagValue(call otto.FunctionCall) otto.Value {
	return p.toValue(call, true)
}
func (p *pathObject) Map(call otto.FunctionCall) otto.Value {
	return p.ForEach(call)
}
func (p *pathObject) ForEach(call otto.FunctionCall) otto.Value {
	it := p.buildIteratorTree()
	it.Tagger().Add(TopResultTag)
	limit := -1
	if len(call.ArgumentList) == 0 {
		return otto.NullValue()
	}
	callback := call.Argument(len(call.ArgumentList) - 1)
	args := exportArgs(call.ArgumentList[:len(call.ArgumentList)-1])
	if len(args) > 1 {
		limit = toInt(args[0])
	}
	p.wk.runIteratorWithCallback(it, callback, call, limit)
	return otto.NullValue()
}

func quadValueToString(v quad.Value) string {
	if s, ok := v.(quad.String); ok {
		return string(s)
	}
	return quad.StringOf(v)
}

func quadValueToNative(v quad.Value) interface{} {
	switch v := v.(type) {
	case quad.String:
		return string(v)
	case quad.Int:
		return int(v)
	case quad.Float:
		return float64(v)
	case quad.Bool:
		return bool(v)
	}
	return quad.StringOf(v)
}

func (wk *worker) tagsToValueMap(m map[string]graph.Value) map[string]interface{} {
	outputMap := make(map[string]interface{})
	for k, v := range m {
		outputMap[k] = quadValueToNative(wk.qs.NameOf(v))
	}
	return outputMap
}

func (wk *worker) runIteratorToArray(it graph.Iterator, limit int) []map[string]interface{} {
	output := make([]map[string]interface{}, 0)
	n := 0
	it, _ = it.Optimize()
	for {
		select {
		case <-wk.kill:
			return nil
		default:
		}
		if !graph.Next(it) {
			break
		}
		tags := make(map[string]graph.Value)
		it.TagResults(tags)
		output = append(output, wk.tagsToValueMap(tags))
		n++
		if limit >= 0 && n >= limit {
			break
		}
		for it.NextPath() {
			select {
			case <-wk.kill:
				return nil
			default:
			}
			tags := make(map[string]graph.Value)
			it.TagResults(tags)
			output = append(output, wk.tagsToValueMap(tags))
			n++
			if limit >= 0 && n >= limit {
				break
			}
		}
	}
	it.Close()
	return output
}

func (wk *worker) runIteratorToArrayNoTags(it graph.Iterator, limit int) []interface{} {
	output := make([]interface{}, 0)
	n := 0
	it, _ = it.Optimize()
	for {
		select {
		case <-wk.kill:
			return nil
		default:
		}
		if !graph.Next(it) {
			break
		}
		output = append(output, quadValueToNative(wk.qs.NameOf(it.Result())))
		n++
		if limit >= 0 && n >= limit {
			break
		}
	}
	it.Close()
	return output
}

func (wk *worker) runIteratorWithCallback(it graph.Iterator, callback otto.Value, this otto.FunctionCall, limit int) {
	n := 0
	it, _ = it.Optimize()
	if clog.V(2) {
		b, err := json.MarshalIndent(it.Describe(), "", "  ")
		if err != nil {
			clog.Infof("failed to format description: %v", err)
		} else {
			clog.Infof("%s", b)
		}
	}
	for {
		select {
		case <-wk.kill:
			return
		default:
		}
		if !graph.Next(it) {
			break
		}
		tags := make(map[string]graph.Value)
		it.TagResults(tags)
		val, _ := this.Otto.ToValue(wk.tagsToValueMap(tags))
		val, _ = callback.Call(this.This, val)
		n++
		if limit >= 0 && n >= limit {
			break
		}
		for it.NextPath() {
			select {
			case <-wk.kill:
				return
			default:
			}
			tags := make(map[string]graph.Value)
			it.TagResults(tags)
			val, _ := this.Otto.ToValue(wk.tagsToValueMap(tags))
			val, _ = callback.Call(this.This, val)
			n++
			if limit >= 0 && n >= limit {
				break
			}
		}
	}
	it.Close()
}

func (wk *worker) send(r *Result) bool {
	if wk.limit >= 0 && wk.limit == wk.count {
		return false
	}
	select {
	case <-wk.kill:
		return false
	default:
	}
	if wk.results != nil {
		wk.results <- r
		wk.count++
		if wk.limit >= 0 && wk.limit == wk.count {
			return false
		}
		return true
	}
	return false
}

func (wk *worker) runIterator(it graph.Iterator) {
	if wk.wantShape() {
		iterator.OutputQueryShapeForIterator(it, wk.qs, wk.shape)
		return
	}
	it, _ = it.Optimize()
	if clog.V(2) {
		b, err := json.MarshalIndent(it.Describe(), "", "  ")
		if err != nil {
			clog.Infof("failed to format description: %v", err)
		} else {
			clog.Infof("%s", b)
		}
	}
	for {
		select {
		case <-wk.kill:
			return
		default:
		}
		if !graph.Next(it) {
			break
		}
		tags := make(map[string]graph.Value)
		it.TagResults(tags)
		if !wk.send(&Result{actualResults: tags}) {
			break
		}
		for it.NextPath() {
			select {
			case <-wk.kill:
				return
			default:
			}
			tags := make(map[string]graph.Value)
			it.TagResults(tags)
			if !wk.send(&Result{actualResults: tags}) {
				break
			}
		}
	}
	if clog.V(2) {
		bytes, _ := json.MarshalIndent(graph.DumpStats(it), "", "  ")
		clog.Infof(string(bytes))
	}
	it.Close()
}
