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
	"fmt"
	"sync"

	"github.com/robertkrimen/otto"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
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

type graphObject struct {
	wk *worker
}

func (g *graphObject) V(call otto.FunctionCall) otto.Value {
	return g.Vertex(call)
}
func (g *graphObject) Vertex(call otto.FunctionCall) otto.Value {
	call.Otto.Run("var out = {}")
	out, err := call.Otto.Object("out")
	if err != nil {
		clog.Errorf("%v",err)
		return otto.TrueValue()
	}
	out.Set("_gremlin_type", "vertex")
	args := argsOf(call)
	if len(args) > 0 {
		out.Set("string_args", args)
	}
	g.wk.embedTraversals(g.wk.env, out)
	g.wk.embedFinals(g.wk.env, out)
	return out.Value()
}
func (g *graphObject) M(call otto.FunctionCall) otto.Value {
	return g.Morphism(call)
}
func (g *graphObject) Morphism(call otto.FunctionCall) otto.Value {
	call.Otto.Run("var out = {}")
	out, _ := call.Otto.Object("out")
	out.Set("_gremlin_type", "morphism")
	g.wk.embedTraversals(g.wk.env, out)
	return out.Value()
}
func (g *graphObject) Emit(call otto.FunctionCall) otto.Value {
	value := call.Argument(0)
	if value.IsDefined() {
		g.wk.send(&Result{val: &value})
	}
	return otto.NullValue()
}

func newWorker(qs graph.QuadStore) *worker {
	env := otto.New()
	wk := &worker{
		qs:    qs,
		env:   env,
		limit: -1,
	}
	env.Set("graph", &graphObject{wk: wk})
	env.Run("g = graph")
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
			switch o := obj.(type) {
			case []interface{}:
				for _, x := range o {
					out = append(out, x.(string))
				}
			case []string:
				for _, x := range o {
					out = append(out, x)
				}
			default:
				panic(fmt.Errorf("unexpected type: %T", obj))
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
