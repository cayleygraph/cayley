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
	"github.com/robertkrimen/otto"
	"golang.org/x/net/context"

	"github.com/codelingo/cayley/clog"
	"github.com/codelingo/cayley/graph"
	"github.com/codelingo/cayley/graph/iterator"
	"github.com/codelingo/cayley/quad"
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
	if len(args) > 1 {
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
	if len(args) > 0 {
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
	out := v.Native()
	if nv, ok := out.(quad.Value); ok && v == nv {
		return quad.StringOf(v)
	}
	return out
}

func (wk *worker) tagsToValueMap(m map[string]graph.Value) map[string]interface{} {
	outputMap := make(map[string]interface{})
	for k, v := range m {
		outputMap[k] = quadValueToNative(wk.qs.NameOf(v))
	}
	return outputMap
}

func (wk *worker) newContext() (context.Context, func()) {
	rctx := context.TODO()
	kill := wk.kill
	ctx, cancel := context.WithCancel(rctx)
	if kill != nil {
		go func() {
			select {
			case <-ctx.Done():
			case <-kill:
				cancel()
			}
		}()
	}
	return ctx, cancel
}

func (wk *worker) runIteratorToArray(it graph.Iterator, limit int) []map[string]interface{} {
	ctx, cancel := wk.newContext()
	defer cancel()

	output := make([]map[string]interface{}, 0)
	err := graph.Iterate(ctx, it).Limit(limit).TagEach(func(tags map[string]graph.Value) {
		output = append(output, wk.tagsToValueMap(tags))
	})
	if err != nil {
		clog.Errorf("gremlin: %v", err)
	}
	return output
}

func (wk *worker) runIteratorToArrayNoTags(it graph.Iterator, limit int) []interface{} {
	ctx, cancel := wk.newContext()
	defer cancel()

	output := make([]interface{}, 0)
	err := graph.Iterate(ctx, it).Paths(false).Limit(limit).EachValue(wk.qs, func(v quad.Value) {
		output = append(output, quadValueToNative(v))
	})
	if err != nil {
		clog.Errorf("gremlin: %v", err)
	}
	return output
}

func (wk *worker) runIteratorWithCallback(it graph.Iterator, callback otto.Value, this otto.FunctionCall, limit int) {
	ctx, cancel := wk.newContext()
	defer cancel()

	err := graph.Iterate(ctx, it).Paths(true).Limit(limit).TagEach(func(tags map[string]graph.Value) {
		val, _ := this.Otto.ToValue(wk.tagsToValueMap(tags))
		val, _ = callback.Call(this.This, val)
	})
	if err != nil {
		clog.Errorf("gremlin: %v", err)
	}
}

func (wk *worker) send(ctx context.Context, r *Result) bool {
	if wk.limit >= 0 && wk.count >= wk.limit {
		return false
	}
	if wk.results == nil {
		return false
	}
	done := wk.kill
	if ctx != nil {
		done = ctx.Done()
	}
	select {
	case wk.results <- r:
	case <-done:
		return false
	}
	wk.count++
	return wk.limit < 0 || wk.count < wk.limit
}

func (wk *worker) runIterator(it graph.Iterator) {
	if wk.wantShape() {
		iterator.OutputQueryShapeForIterator(it, wk.qs, wk.shape)
		return
	}

	ctx, cancel := wk.newContext()
	defer cancel()

	err := graph.Iterate(ctx, it).Paths(true).TagEach(func(tags map[string]graph.Value) {
		if !wk.send(ctx, &Result{actualResults: tags}) {
			cancel()
		}
	})
	if err != nil {
		clog.Errorf("gremlin: %v", err)
	}
}
