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
	"time"

	"github.com/robertkrimen/otto"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/path"
	"github.com/cayleygraph/cayley/quad"
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
	qv := toQuadValues(exportArgs(call.ArgumentList))
	return outObj(call, &pathObject{
		wk:     g.wk,
		finals: true,
		path:   path.StartMorphism(qv...),
	})
}
func (g *graphObject) M(call otto.FunctionCall) otto.Value {
	return g.Morphism(call)
}
func (g *graphObject) Morphism(call otto.FunctionCall) otto.Value {
	return outObj(call, &pathObject{
		wk:   g.wk,
		path: path.StartMorphism(),
	})
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

func exportAsPath(args []otto.Value) *pathObject {
	if len(args) == 0 {
		return nil
	}
	o, _ := args[0].Export()
	return o.(*pathObject)
}

func exportArgs(args []otto.Value) []interface{} {
	if len(args) == 0 {
		return nil
	}
	out := make([]interface{}, 0, len(args))
	for _, a := range args {
		if a.IsObject() && a.Class() == "Date" {
			ms, _ := a.Object().Call("getTime")
			msi, _ := ms.ToInteger()
			t := time.Unix(msi/1000, (msi%1000)*1e6)
			out = append(out, t)
		} else {
			o, _ := a.Export()
			out = append(out, o)
		}
	}
	return out
}

func toInt(o interface{}) int {
	switch v := o.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}

func toQuadValue(o interface{}) (quad.Value, bool) {
	var qv quad.Value
	switch v := o.(type) {
	case quad.Value:
		qv = v
	case string:
		qv = quad.Raw(v)
	case bool:
		qv = quad.Bool(v)
	case int:
		qv = quad.Int(v)
	case int64:
		qv = quad.Int(v)
	case float64:
		if float64(int(v)) == v {
			qv = quad.Int(int64(v))
		} else {
			qv = quad.Float(v)
		}
	case time.Time:
		qv = quad.Time(v)
	default:
		return nil, false
	}
	return qv, true
}

func toQuadValues(objs []interface{}) []quad.Value {
	if len(objs) == 0 {
		return nil
	}
	vals := make([]quad.Value, 0, len(objs))
	for _, o := range objs {
		qv, ok := toQuadValue(o)
		if !ok {
			panic(fmt.Errorf("unsupported type: %T", o))
		}
		vals = append(vals, qv)
	}
	return vals
}

func toStrings(objs []interface{}) []string {
	if len(objs) == 0 {
		return nil
	}
	var out = make([]string, 0, len(objs))
	for _, o := range objs {
		switch v := o.(type) {
		case string:
			out = append(out, v)
		case quad.Value:
			out = append(out, quad.StringOf(v))
		case []string:
			out = append(out, v...)
		case []interface{}:
			out = append(out, toStrings(v)...)
		default:
			panic(fmt.Errorf("expected string, got: %T", o))
		}
	}
	return out
}

func toVia(via []interface{}) []interface{} {
	if len(via) == 0 {
		return nil
	} else if len(via) == 1 {
		if via[0] == nil {
			return nil
		} else if v, ok := via[0].([]interface{}); ok {
			return toVia(v)
		}
	}
	for i := range via {
		if vp, ok := via[i].(*pathObject); ok {
			via[i] = vp.path
		} else if qv, ok := toQuadValue(via[i]); ok {
			via[i] = qv
		} else {
			panic(fmt.Errorf("unsupported type: %T", via[i]))
		}
	}
	return via
}

func toViaData(objs []interface{}) (predicates []interface{}, tags []string, ok bool) {
	if len(objs) != 0 {
		predicates = toVia([]interface{}{objs[0]})
	}
	if len(objs) >= 2 {
		tags = toStrings(objs[1:])
	}
	ok = true
	return
}

func outObj(call otto.FunctionCall, o interface{}) otto.Value {
	call.Otto.Set("out", o)
	v, _ := call.Otto.Get("out")
	return v
}
