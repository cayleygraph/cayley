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

// Builds a new Gremlin environment pointing at a session.

import (
	"github.com/barakmich/glog"
	"github.com/robertkrimen/otto"
)

func BuildEnviron(ses *Session) *otto.Otto {
	env := otto.New()
	setupGremlin(env, ses)
	return env
}

func concatStringArgs(call otto.FunctionCall) *[]interface{} {
	outStrings := make([]interface{}, 0)
	for _, arg := range call.ArgumentList {
		if arg.IsString() {
			outStrings = append(outStrings, arg.String())
		}
		if arg.IsObject() && arg.Class() == "Array" {
			obj, _ := arg.Export()
			for _, x := range obj.([]interface{}) {
				outStrings = append(outStrings, x.(string))
			}
		}
	}
	return &outStrings
}

func isVertexChain(obj *otto.Object) bool {
	val, _ := obj.Get("_gremlin_type")
	if x, _ := val.ToString(); x == "vertex" {
		return true
	}
	val, _ = obj.Get("_gremlin_prev")
	if val.IsObject() {
		return isVertexChain(val.Object())
	}
	return false
}

func setupGremlin(env *otto.Otto, ses *Session) {
	graph, _ := env.Object("graph = {}")
	graph.Set("Vertex", func(call otto.FunctionCall) otto.Value {
		call.Otto.Run("var out = {}")
		out, err := call.Otto.Object("out")
		if err != nil {
			glog.Error(err.Error())
			return otto.TrueValue()
		}
		out.Set("_gremlin_type", "vertex")
		outStrings := concatStringArgs(call)
		if len(*outStrings) > 0 {
			out.Set("string_args", *outStrings)
		}
		embedTraversals(env, ses, out)
		embedFinals(env, ses, out)
		return out.Value()
	})

	graph.Set("Morphism", func(call otto.FunctionCall) otto.Value {
		call.Otto.Run("var out = {}")
		out, _ := call.Otto.Object("out")
		out.Set("_gremlin_type", "morphism")
		embedTraversals(env, ses, out)
		return out.Value()
	})
	graph.Set("Emit", func(call otto.FunctionCall) otto.Value {
		value := call.Argument(0)
		if value.IsDefined() {
			ses.SendResult(&GremlinResult{metaresult: false, err: nil, val: &value, actualResults: nil})
		}
		return otto.NullValue()
	})
	env.Run("graph.V = graph.Vertex")
	env.Run("graph.M = graph.Morphism")
	env.Run("g = graph")

}
