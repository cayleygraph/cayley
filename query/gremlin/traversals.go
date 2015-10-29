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

var traversals = []string{
	"In",
	"Out",
	"Is",
	"Both",
	"Follow",
	"FollowR",
	"And",
	"Intersect",
	"Union",
	"Or",
	"Back",
	"Tag",
	"As",
	"Has",
	"Save",
	"SaveR",
	"Except",
	"Difference",
	"InPredicates",
	"OutPredicates",
}

func (wk *worker) embedTraversals(env *otto.Otto, obj *otto.Object) {
	for _, t := range traversals {
		obj.Set(t, wk.gremlinFunc(t, obj, env))
	}
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

func debugChain(obj *otto.Object) bool {
	val, _ := obj.Get("_gremlin_type")
	glog.V(2).Infoln(val)
	val, _ = obj.Get("_gremlin_prev")
	if val.IsObject() {
		return debugChain(val.Object())
	}
	return false
}
