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

// Adds special traversal functions to JS Gremlin objects. Most of these just build the chain of objects, and won't often need the session.

import (
	"github.com/barakmich/glog"
	"github.com/robertkrimen/otto"
)

func (wk *worker) embedTraversals(env *otto.Otto, obj *otto.Object) {
	obj.Set("In", wk.gremlinFunc("in", obj, env))
	obj.Set("Out", wk.gremlinFunc("out", obj, env))
	obj.Set("Is", wk.gremlinFunc("is", obj, env))
	obj.Set("Both", wk.gremlinFunc("both", obj, env))
	obj.Set("Follow", wk.gremlinFunc("follow", obj, env))
	obj.Set("FollowR", wk.gremlinFollowR("followr", obj, env))
	obj.Set("And", wk.gremlinFunc("and", obj, env))
	obj.Set("Intersect", wk.gremlinFunc("and", obj, env))
	obj.Set("Union", wk.gremlinFunc("or", obj, env))
	obj.Set("Or", wk.gremlinFunc("or", obj, env))
	obj.Set("Back", wk.gremlinBack("back", obj, env))
	obj.Set("Tag", wk.gremlinFunc("tag", obj, env))
	obj.Set("As", wk.gremlinFunc("tag", obj, env))
	obj.Set("Has", wk.gremlinFunc("has", obj, env))
	obj.Set("Save", wk.gremlinFunc("save", obj, env))
	obj.Set("SaveR", wk.gremlinFunc("saver", obj, env))
	obj.Set("Except", wk.gremlinFunc("except", obj, env))
	obj.Set("Difference", wk.gremlinFunc("except", obj, env))
	obj.Set("InPredicates", wk.gremlinFunc("in_predicates", obj, env))
	obj.Set("OutPredicates", wk.gremlinFunc("out_predicates", obj, env))
}

func (wk *worker) gremlinFunc(kind string, prev *otto.Object, env *otto.Otto) func(otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) otto.Value {
		call.Otto.Run("var out = {}")
		out, _ := call.Otto.Object("out")
		out.Set("_gremlin_type", kind)
		out.Set("_gremlin_values", call.ArgumentList)
		out.Set("_gremlin_prev", prev)
		args := argsOf(call)
		if len(args) > 0 {
			out.Set("string_args", args)
		}
		wk.embedTraversals(env, out)
		if isVertexChain(call.This.Object()) {
			wk.embedFinals(env, out)
		}
		return out.Value()
	}
}

func (wk *worker) gremlinBack(kind string, prev *otto.Object, env *otto.Otto) func(otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) otto.Value {
		call.Otto.Run("var out = {}")
		out, _ := call.Otto.Object("out")
		out.Set("_gremlin_type", kind)
		out.Set("_gremlin_values", call.ArgumentList)
		args := argsOf(call)
		if len(args) > 0 {
			out.Set("string_args", args)
		}
		var otherChain *otto.Object
		var thisObj *otto.Object
		if len(args) != 0 {
			otherChain, thisObj = reverseGremlinChainTo(call.Otto, prev, args[0])
		} else {
			otherChain, thisObj = reverseGremlinChainTo(call.Otto, prev, "")
		}
		out.Set("_gremlin_prev", thisObj)
		out.Set("_gremlin_back_chain", otherChain)
		wk.embedTraversals(env, out)
		if isVertexChain(call.This.Object()) {
			wk.embedFinals(env, out)
		}
		return out.Value()
	}
}

func (wk *worker) gremlinFollowR(kind string, prev *otto.Object, env *otto.Otto) func(otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) otto.Value {
		call.Otto.Run("var out = {}")
		out, _ := call.Otto.Object("out")
		out.Set("_gremlin_type", kind)
		out.Set("_gremlin_values", call.ArgumentList)
		args := argsOf(call)
		if len(args) > 0 {
			out.Set("string_args", args)
		}
		if len(call.ArgumentList) == 0 {
			return prev.Value()
		}
		arg := call.Argument(0)
		if isVertexChain(arg.Object()) {
			return prev.Value()
		}
		newChain, _ := reverseGremlinChainTo(call.Otto, arg.Object(), "")
		out.Set("_gremlin_prev", prev)
		out.Set("_gremlin_followr", newChain)
		wk.embedTraversals(env, out)
		if isVertexChain(call.This.Object()) {
			wk.embedFinals(env, out)
		}
		return out.Value()
	}
}

func reverseGremlinChainTo(env *otto.Otto, prev *otto.Object, tag string) (*otto.Object, *otto.Object) {
	env.Run("var _base_object = {}")
	base, err := env.Object("_base_object")
	if err != nil {
		glog.Error(err)
		return otto.NullValue().Object(), otto.NullValue().Object()
	}
	if isVertexChain(prev) {
		base.Set("_gremlin_type", "vertex")
	} else {
		base.Set("_gremlin_type", "morphism")
	}
	return reverseGremlinChainHelper(env, prev, base, tag)
}

func reverseGremlinChainHelper(env *otto.Otto, chain *otto.Object, newBase *otto.Object, tag string) (*otto.Object, *otto.Object) {
	kindVal, _ := chain.Get("_gremlin_type")
	kind := kindVal.String()

	if tag != "" {
		if kind == "tag" {
			tags := propertiesOf(chain, "string_args")
			for _, t := range tags {
				if t == tag {
					return newBase, chain
				}
			}
		}
	}

	if kind == "morphism" || kind == "vertex" {
		return newBase, chain
	}
	var newKind string
	switch kind {
	case "in":
		newKind = "out"
	case "out":
		newKind = "in"
	default:
		newKind = kind
	}
	prev, _ := chain.Get("_gremlin_prev")
	env.Run("var out = {}")
	out, _ := env.Object("out")
	out.Set("_gremlin_type", newKind)
	values, _ := chain.Get("_gremlin_values")
	out.Set("_gremlin_values", values)
	back, _ := chain.Get("_gremlin_back_chain")
	out.Set("_gremlin_back_chain", back)
	out.Set("_gremlin_prev", newBase)
	strings, _ := chain.Get("string_args")
	out.Set("string_args", strings)
	return reverseGremlinChainHelper(env, prev.Object(), out, tag)
}

func debugChain(obj *otto.Object) bool {
	val, _ := obj.Get("_gremlin_type")
	glog.V(2).Infoln(val)
	val, _ = obj.Get("_gremlin_prev")
	if val.IsObject() {
		return debugChain(val.Object())
	}
	return false
}
