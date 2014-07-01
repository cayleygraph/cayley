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
	"github.com/barakmich/glog"
	"github.com/robertkrimen/otto"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
)

const TopResultTag = "id"

func embedFinals(env *otto.Otto, ses *Session, obj *otto.Object) {
	obj.Set("All", allFunc(env, ses, obj))
	obj.Set("GetLimit", limitFunc(env, ses, obj))
	obj.Set("ToArray", toArrayFunc(env, ses, obj, false))
	obj.Set("ToValue", toValueFunc(env, ses, obj, false))
	obj.Set("TagArray", toArrayFunc(env, ses, obj, true))
	obj.Set("TagValue", toValueFunc(env, ses, obj, true))
	obj.Set("Map", mapFunc(env, ses, obj))
	obj.Set("ForEach", mapFunc(env, ses, obj))
}

func allFunc(env *otto.Otto, ses *Session, obj *otto.Object) func(otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) otto.Value {
		it := buildIteratorTree(obj, ses.ts)
		it.AddTag(TopResultTag)
		ses.limit = -1
		ses.count = 0
		runIteratorOnSession(it, ses)
		return otto.NullValue()
	}
}

func limitFunc(env *otto.Otto, ses *Session, obj *otto.Object) func(otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) otto.Value {
		if len(call.ArgumentList) > 0 {
			limitVal, _ := call.Argument(0).ToInteger()
			it := buildIteratorTree(obj, ses.ts)
			it.AddTag(TopResultTag)
			ses.limit = int(limitVal)
			ses.count = 0
			runIteratorOnSession(it, ses)
		}
		return otto.NullValue()
	}
}

func toArrayFunc(env *otto.Otto, ses *Session, obj *otto.Object, withTags bool) func(otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) otto.Value {
		it := buildIteratorTree(obj, ses.ts)
		it.AddTag(TopResultTag)
		limit := -1
		if len(call.ArgumentList) > 0 {
			limitParsed, _ := call.Argument(0).ToInteger()
			limit = int(limitParsed)
		}
		var val otto.Value
		var err error
		if !withTags {
			array := runIteratorToArrayNoTags(it, ses, limit)
			val, err = call.Otto.ToValue(array)
		} else {
			array := runIteratorToArray(it, ses, limit)
			val, err = call.Otto.ToValue(array)
		}

		if err != nil {
			glog.Error(err)
			return otto.NullValue()
		}
		return val
	}
}

func toValueFunc(env *otto.Otto, ses *Session, obj *otto.Object, withTags bool) func(otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) otto.Value {
		it := buildIteratorTree(obj, ses.ts)
		it.AddTag(TopResultTag)
		limit := 1
		var val otto.Value
		var err error
		if !withTags {
			array := runIteratorToArrayNoTags(it, ses, limit)
			if len(array) < 1 {
				return otto.NullValue()
			}
			val, err = call.Otto.ToValue(array[0])
		} else {
			array := runIteratorToArray(it, ses, limit)
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

func mapFunc(env *otto.Otto, ses *Session, obj *otto.Object) func(otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) otto.Value {
		it := buildIteratorTree(obj, ses.ts)
		it.AddTag(TopResultTag)
		limit := -1
		if len(call.ArgumentList) == 0 {
			return otto.NullValue()
		}
		callback := call.Argument(len(call.ArgumentList) - 1)
		if len(call.ArgumentList) > 1 {
			limitParsed, _ := call.Argument(0).ToInteger()
			limit = int(limitParsed)
		}
		runIteratorWithCallback(it, ses, callback, call, limit)
		return otto.NullValue()
	}
}

func tagsToValueMap(m map[string]graph.TSVal, ses *Session) map[string]string {
	outputMap := make(map[string]string)
	for k, v := range m {
		outputMap[k] = ses.ts.GetNameFor(v)
	}
	return outputMap
}

func runIteratorToArray(it graph.Iterator, ses *Session, limit int) []map[string]string {
	output := make([]map[string]string, 0)
	count := 0
	it, _ = it.Optimize()
	for {
		if ses.doHalt {
			return nil
		}
		_, ok := it.Next()
		if !ok {
			break
		}
		tags := make(map[string]graph.TSVal)
		it.TagResults(&tags)
		output = append(output, tagsToValueMap(tags, ses))
		count++
		if limit >= 0 && count >= limit {
			break
		}
		for it.NextResult() == true {
			if ses.doHalt {
				return nil
			}
			tags := make(map[string]graph.TSVal)
			it.TagResults(&tags)
			output = append(output, tagsToValueMap(tags, ses))
			count++
			if limit >= 0 && count >= limit {
				break
			}
		}
	}
	it.Close()
	return output
}

func runIteratorToArrayNoTags(it graph.Iterator, ses *Session, limit int) []string {
	output := make([]string, 0)
	count := 0
	it, _ = it.Optimize()
	for {
		if ses.doHalt {
			return nil
		}
		val, ok := it.Next()
		if !ok {
			break
		}
		output = append(output, ses.ts.GetNameFor(val))
		count++
		if limit >= 0 && count >= limit {
			break
		}
	}
	it.Close()
	return output
}

func runIteratorWithCallback(it graph.Iterator, ses *Session, callback otto.Value, this otto.FunctionCall, limit int) {
	count := 0
	it, _ = it.Optimize()
	for {
		if ses.doHalt {
			return
		}
		_, ok := it.Next()
		if !ok {
			break
		}
		tags := make(map[string]graph.TSVal)
		it.TagResults(&tags)
		val, _ := this.Otto.ToValue(tagsToValueMap(tags, ses))
		val, _ = callback.Call(this.This, val)
		count++
		if limit >= 0 && count >= limit {
			break
		}
		for it.NextResult() == true {
			if ses.doHalt {
				return
			}
			tags := make(map[string]graph.TSVal)
			it.TagResults(&tags)
			val, _ := this.Otto.ToValue(tagsToValueMap(tags, ses))
			val, _ = callback.Call(this.This, val)
			count++
			if limit >= 0 && count >= limit {
				break
			}
		}
	}
	it.Close()
}

func runIteratorOnSession(it graph.Iterator, ses *Session) {
	if ses.lookingForQueryShape {
		iterator.OutputQueryShapeForIterator(it, ses.ts, &(ses.queryShape))
		return
	}
	it, _ = it.Optimize()
	glog.V(2).Infoln(it.DebugString(0))
	for {
		// TODO(barakmich): Better halting.
		if ses.doHalt {
			return
		}
		_, ok := it.Next()
		if !ok {
			break
		}
		tags := make(map[string]graph.TSVal)
		it.TagResults(&tags)
		cont := ses.SendResult(&GremlinResult{metaresult: false, err: "", val: nil, actualResults: &tags})
		if !cont {
			break
		}
		for it.NextResult() == true {
			if ses.doHalt {
				return
			}
			tags := make(map[string]graph.TSVal)
			it.TagResults(&tags)
			cont := ses.SendResult(&GremlinResult{metaresult: false, err: "", val: nil, actualResults: &tags})
			if !cont {
				break
			}
		}
	}
	it.Close()
}
