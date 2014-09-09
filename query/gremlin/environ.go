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
	"sync"

	"github.com/barakmich/glog"
	"github.com/robertkrimen/otto"

	"github.com/google/cayley/graph"
)

type worker struct {
	qs  graph.QuadStore
	env *otto.Otto
	sync.Mutex

	results chan interface{}
	shape   map[string]interface{}

	count int
	limit int

	kill <-chan struct{}
}

func newWorker(qs graph.QuadStore) *worker {
	env := otto.New()
	wk := &worker{
		qs:    qs,
		env:   env,
		limit: -1,
	}
	graph, _ := env.Object("graph = {}")
	env.Run("g = graph")

	graph.Set("Vertex", func(call otto.FunctionCall) otto.Value {
		call.Otto.Run("var out = {}")
		out, err := call.Otto.Object("out")
		if err != nil {
			glog.Error(err.Error())
			return otto.TrueValue()
		}
		out.Set("_gremlin_type", "vertex")
		args := argsOf(call)
		if len(args) > 0 {
			out.Set("string_args", args)
		}
		wk.embedTraversals(env, out)
		wk.embedFinals(env, out)
		return out.Value()
	})
	env.Run("graph.V = graph.Vertex")

	graph.Set("Morphism", func(call otto.FunctionCall) otto.Value {
		call.Otto.Run("var out = {}")
		out, _ := call.Otto.Object("out")
		out.Set("_gremlin_type", "morphism")
		wk.embedTraversals(env, out)
		return out.Value()
	})
	env.Run("graph.M = graph.Morphism")

	graph.Set("Emit", func(call otto.FunctionCall) otto.Value {
		value := call.Argument(0)
		if value.IsDefined() {
			wk.send(&Result{val: &value})
		}
		return otto.NullValue()
	})

	return wk
}

func (wk *worker) wantShape() bool {
	return wk.shape != nil
}

func argsOf(call otto.FunctionCall) []string {
	var out []string
	for _, arg := range call.ArgumentList {
		if arg.IsString() {
			out = append(out, arg.String())
		}
		if arg.IsObject() && arg.Class() == "Array" {
			obj, _ := arg.Export()
			for _, x := range obj.([]interface{}) {
				out = append(out, x.(string))
			}
		}
	}
	return out
}

func isVertexChain(obj *otto.Object) bool {
	val, _ := obj.Get("_gremlin_type")
	if val.String() == "vertex" {
		return true
	}
	val, _ = obj.Get("_gremlin_prev")
	if val.IsObject() {
		return isVertexChain(val.Object())
	}
	return false
}
