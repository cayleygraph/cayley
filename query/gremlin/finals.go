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

func (s *Session) embedFinals(env *otto.Otto, obj *otto.Object) {
	obj.Set("All", s.allFunc(env, obj))
	obj.Set("GetLimit", s.limitFunc(env, obj))
	obj.Set("ToArray", s.toArrayFunc(env, obj, false))
	obj.Set("ToValue", s.toValueFunc(env, obj, false))
	obj.Set("TagArray", s.toArrayFunc(env, obj, true))
	obj.Set("TagValue", s.toValueFunc(env, obj, true))
	obj.Set("Map", s.mapFunc(env, obj))
	obj.Set("ForEach", s.mapFunc(env, obj))
}

func (s *Session) allFunc(env *otto.Otto, obj *otto.Object) func(otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) otto.Value {
		it := buildIteratorTree(obj, s.ts)
		it.Tagger().Add(TopResultTag)
		s.limit = -1
		s.count = 0
		s.runIterator(it)
		return otto.NullValue()
	}
}

func (s *Session) limitFunc(env *otto.Otto, obj *otto.Object) func(otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) otto.Value {
		if len(call.ArgumentList) > 0 {
			limitVal, _ := call.Argument(0).ToInteger()
			it := buildIteratorTree(obj, s.ts)
			it.Tagger().Add(TopResultTag)
			s.limit = int(limitVal)
			s.count = 0
			s.runIterator(it)
		}
		return otto.NullValue()
	}
}

func (s *Session) toArrayFunc(env *otto.Otto, obj *otto.Object, withTags bool) func(otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) otto.Value {
		it := buildIteratorTree(obj, s.ts)
		it.Tagger().Add(TopResultTag)
		limit := -1
		if len(call.ArgumentList) > 0 {
			limitParsed, _ := call.Argument(0).ToInteger()
			limit = int(limitParsed)
		}
		var val otto.Value
		var err error
		if !withTags {
			array := s.runIteratorToArrayNoTags(it, limit)
			val, err = call.Otto.ToValue(array)
		} else {
			array := s.runIteratorToArray(it, limit)
			val, err = call.Otto.ToValue(array)
		}

		if err != nil {
			glog.Error(err)
			return otto.NullValue()
		}
		return val
	}
}

func (s *Session) toValueFunc(env *otto.Otto, obj *otto.Object, withTags bool) func(otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) otto.Value {
		it := buildIteratorTree(obj, s.ts)
		it.Tagger().Add(TopResultTag)
		limit := 1
		var val otto.Value
		var err error
		if !withTags {
			array := s.runIteratorToArrayNoTags(it, limit)
			if len(array) < 1 {
				return otto.NullValue()
			}
			val, err = call.Otto.ToValue(array[0])
		} else {
			array := s.runIteratorToArray(it, limit)
			if len(array) < 1 {
				return otto.NullValue()
			}
			val, err = call.Otto.ToValue(array[0])
		}
		if err != nil {
			glog.Error(err)
			return otto.NullValue()
		} else {
			return val
		}

	}
}

func (s *Session) mapFunc(env *otto.Otto, obj *otto.Object) func(otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) otto.Value {
		it := buildIteratorTree(obj, s.ts)
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
		s.runIteratorWithCallback(it, callback, call, limit)
		return otto.NullValue()
	}
}

func (s *Session) tagsToValueMap(m map[string]graph.Value) map[string]string {
	outputMap := make(map[string]string)
	for k, v := range m {
		outputMap[k] = s.ts.NameOf(v)
	}
	return outputMap
}

func (s *Session) runIteratorToArray(it graph.Iterator, limit int) []map[string]string {
	output := make([]map[string]string, 0)
	count := 0
	it, _ = it.Optimize()
	for {
		select {
		case <-s.kill:
			return nil
		default:
		}
		if !graph.Next(it) {
			break
		}
		tags := make(map[string]graph.Value)
		it.TagResults(tags)
		output = append(output, s.tagsToValueMap(tags))
		count++
		if limit >= 0 && count >= limit {
			break
		}
		for it.NextPath() {
			select {
			case <-s.kill:
				return nil
			default:
			}
			tags := make(map[string]graph.Value)
			it.TagResults(tags)
			output = append(output, s.tagsToValueMap(tags))
			count++
			if limit >= 0 && count >= limit {
				break
			}
		}
	}
	it.Close()
	return output
}

func (s *Session) runIteratorToArrayNoTags(it graph.Iterator, limit int) []string {
	output := make([]string, 0)
	count := 0
	it, _ = it.Optimize()
	for {
		select {
		case <-s.kill:
			return nil
		default:
		}
		if !graph.Next(it) {
			break
		}
		output = append(output, s.ts.NameOf(it.Result()))
		count++
		if limit >= 0 && count >= limit {
			break
		}
	}
	it.Close()
	return output
}

func (s *Session) runIteratorWithCallback(it graph.Iterator, callback otto.Value, this otto.FunctionCall, limit int) {
	count := 0
	it, _ = it.Optimize()
	glog.V(2).Infoln(it.DebugString(0))
	for {
		select {
		case <-s.kill:
			return
		default:
		}
		if !graph.Next(it) {
			break
		}
		tags := make(map[string]graph.Value)
		it.TagResults(tags)
		val, _ := this.Otto.ToValue(s.tagsToValueMap(tags))
		val, _ = callback.Call(this.This, val)
		count++
		if limit >= 0 && count >= limit {
			break
		}
		for it.NextPath() {
			select {
			case <-s.kill:
				return
			default:
			}
			tags := make(map[string]graph.Value)
			it.TagResults(tags)
			val, _ := this.Otto.ToValue(s.tagsToValueMap(tags))
			val, _ = callback.Call(this.This, val)
			count++
			if limit >= 0 && count >= limit {
				break
			}
		}
	}
	it.Close()
}

func (s *Session) runIterator(it graph.Iterator) {
	if s.wantShape {
		iterator.OutputQueryShapeForIterator(it, s.ts, s.shape)
		return
	}
	it, _ = it.Optimize()
	glog.V(2).Infoln(it.DebugString(0))
	for {
		select {
		case <-s.kill:
			return
		default:
		}
		if !graph.Next(it) {
			break
		}
		tags := make(map[string]graph.Value)
		it.TagResults(tags)
		if !s.SendResult(&Result{actualResults: &tags}) {
			break
		}
		for it.NextPath() {
			select {
			case <-s.kill:
				return
			default:
			}
			tags := make(map[string]graph.Value)
			it.TagResults(tags)
			if !s.SendResult(&Result{actualResults: &tags}) {
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
