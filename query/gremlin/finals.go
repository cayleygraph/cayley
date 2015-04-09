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

	"github.com/barakmich/glog"
	"github.com/robertkrimen/otto"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
)

const TopResultTag = "id"

func (wk *worker) embedFinals(env *otto.Otto, obj *otto.Object) {
	obj.Set("All", wk.allFunc(env, obj))
	obj.Set("GetLimit", wk.limitFunc(env, obj))
	obj.Set("ToArray", wk.toArrayFunc(env, obj, false))
	obj.Set("ToValue", wk.toValueFunc(env, obj, false))
	obj.Set("TagArray", wk.toArrayFunc(env, obj, true))
	obj.Set("TagValue", wk.toValueFunc(env, obj, true))
	obj.Set("Map", wk.mapFunc(env, obj))
	obj.Set("ForEach", wk.mapFunc(env, obj))
}

func (wk *worker) allFunc(env *otto.Otto, obj *otto.Object) func(otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) otto.Value {
		it := buildIteratorTree(obj, wk.qs)
		it.Tagger().Add(TopResultTag)
		wk.limit = -1
		wk.count = 0
		wk.runIterator(it)
		return otto.NullValue()
	}
}

func (wk *worker) limitFunc(env *otto.Otto, obj *otto.Object) func(otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) otto.Value {
		if len(call.ArgumentList) > 0 {
			limitVal, _ := call.Argument(0).ToInteger()
			it := buildIteratorTree(obj, wk.qs)
			it.Tagger().Add(TopResultTag)
			wk.limit = int(limitVal)
			wk.count = 0
			wk.runIterator(it)
		}
		return otto.NullValue()
	}
}

func (wk *worker) toArrayFunc(env *otto.Otto, obj *otto.Object, withTags bool) func(otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) otto.Value {
		it := buildIteratorTree(obj, wk.qs)
		it.Tagger().Add(TopResultTag)
		limit := -1
		if len(call.ArgumentList) > 0 {
			limitParsed, _ := call.Argument(0).ToInteger()
			limit = int(limitParsed)
		}
		var val otto.Value
		var err error
		if !withTags {
			array := wk.runIteratorToArrayNoTags(it, limit)
			val, err = call.Otto.ToValue(array)
		} else {
			array := wk.runIteratorToArray(it, limit)
			val, err = call.Otto.ToValue(array)
		}

		if err != nil {
			glog.Error(err)
			return otto.NullValue()
		}
		return val
	}
}

func (wk *worker) toValueFunc(env *otto.Otto, obj *otto.Object, withTags bool) func(otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) otto.Value {
		it := buildIteratorTree(obj, wk.qs)
		it.Tagger().Add(TopResultTag)
		limit := 1
		var val otto.Value
		var err error
		if !withTags {
			array := wk.runIteratorToArrayNoTags(it, limit)
			if len(array) < 1 {
				return otto.NullValue()
			}
			val, err = call.Otto.ToValue(array[0])
		} else {
			array := wk.runIteratorToArray(it, limit)
			if len(array) < 1 {
				return otto.NullValue()
			}
			val, err = call.Otto.ToValue(array[0])
		}
		if err != nil {
			glog.Error(err)
			return otto.NullValue()
		}
		return val
	}
}

func (wk *worker) mapFunc(env *otto.Otto, obj *otto.Object) func(otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) otto.Value {
		it := buildIteratorTree(obj, wk.qs)
		it.Tagger().Add(TopResultTag)
		limit := -1
		if len(call.ArgumentList) == 0 {
			return otto.NullValue()
		}
		callback := call.Argument(len(call.ArgumentList) - 1)
		if len(call.ArgumentList) > 1 {
			limitParsed, _ := call.Argument(0).ToInteger()
			limit = int(limitParsed)
		}
		wk.runIteratorWithCallback(it, callback, call, limit)
		return otto.NullValue()
	}
}

func (wk *worker) tagsToValueMap(m map[string]graph.Value) map[string]string {
	outputMap := make(map[string]string)
	for k, v := range m {
		outputMap[k] = wk.qs.NameOf(v)
	}
	return outputMap
}

func (wk *worker) runIteratorToArray(it graph.Iterator, limit int) []map[string]string {
	output := make([]map[string]string, 0)
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

func (wk *worker) runIteratorToArrayNoTags(it graph.Iterator, limit int) []string {
	output := make([]string, 0)
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
		output = append(output, wk.qs.NameOf(it.Result()))
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
	if glog.V(2) {
		b, err := json.MarshalIndent(it.Describe(), "", "  ")
		if err != nil {
			glog.V(2).Infof("failed to format description: %v", err)
		} else {
			glog.V(2).Infof("%s", b)
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
	if glog.V(2) {
		b, err := json.MarshalIndent(it.Describe(), "", "  ")
		if err != nil {
			glog.V(2).Infof("failed to format description: %v", err)
		} else {
			glog.V(2).Infof("%s", b)
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
	if glog.V(2) {
		bytes, _ := json.MarshalIndent(graph.DumpStats(it), "", "  ")
		glog.V(2).Infoln(string(bytes))
	}
	it.Close()
}
