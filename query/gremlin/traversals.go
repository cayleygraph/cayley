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

func (s *Session) embedTraversals(env *otto.Otto, obj *otto.Object) {
	obj.Set("In", gremlinFunc("in", obj, env, s))
	obj.Set("Out", gremlinFunc("out", obj, env, s))
	obj.Set("Is", gremlinFunc("is", obj, env, s))
	obj.Set("Both", gremlinFunc("both", obj, env, s))
	obj.Set("Follow", gremlinFunc("follow", obj, env, s))
	obj.Set("FollowR", gremlinFollowR("followr", obj, env, s))
	obj.Set("And", gremlinFunc("and", obj, env, s))
	obj.Set("Intersect", gremlinFunc("and", obj, env, s))
	obj.Set("Union", gremlinFunc("or", obj, env, s))
	obj.Set("Or", gremlinFunc("or", obj, env, s))
	obj.Set("Back", gremlinBack("back", obj, env, s))
	obj.Set("Tag", gremlinFunc("tag", obj, env, s))
	obj.Set("As", gremlinFunc("tag", obj, env, s))
	obj.Set("Has", gremlinFunc("has", obj, env, s))
	obj.Set("Save", gremlinFunc("save", obj, env, s))
	obj.Set("SaveR", gremlinFunc("saver", obj, env, s))
}

func gremlinFunc(kind string, prev *otto.Object, env *otto.Otto, ses *Session) func(otto.FunctionCall) otto.Value {
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
		ses.embedTraversals(env, out)
		if isVertexChain(call.This.Object()) {
			ses.embedFinals(env, out)
		}
		return out.Value()
	}
}

func gremlinBack(kind string, prev *otto.Object, env *otto.Otto, ses *Session) func(otto.FunctionCall) otto.Value {
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
		ses.embedTraversals(env, out)
		if isVertexChain(call.This.Object()) {
			ses.embedFinals(env, out)
		}
		return out.Value()

	}
}

func gremlinFollowR(kind string, prev *otto.Object, env *otto.Otto, ses *Session) func(otto.FunctionCall) otto.Value {
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
		ses.embedTraversals(env, out)
		if isVertexChain(call.This.Object()) {
			ses.embedFinals(env, out)
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
