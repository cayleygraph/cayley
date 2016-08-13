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

	"github.com/codelingo/cayley/graph"
	"github.com/codelingo/cayley/graph/iterator"
	"github.com/codelingo/cayley/graph/path"
	"github.com/codelingo/cayley/quad"
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
	qv, err := toQuadValues(exportArgs(call.ArgumentList))
	if err != nil {
		//TODO(dennwc): pass error to caller
		return otto.NullValue()
	}
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
		val := exportArgs([]otto.Value{value})[0]
		if val != nil {
			g.wk.send(nil, &Result{val: val})
		}
	}
	return otto.NullValue()
}

func oneStringType(fnc func(s string) quad.Value) func(call otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) otto.Value {
		args := toStrings(exportArgs(call.ArgumentList))
		if len(args) != 1 {
			return otto.NullValue()
		}
		return outObj(call, quadValue{fnc(args[0])})
	}
}

func twoStringType(fnc func(s1, s2 string) quad.Value) func(call otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) otto.Value {
		args := toStrings(exportArgs(call.ArgumentList))
		if len(args) != 2 {
			return otto.NullValue()
		}
		return outObj(call, quadValue{fnc(args[0], args[1])})
	}
}

func cmpOpType(op iterator.Operator) func(call otto.FunctionCall) otto.Value {
	return func(call otto.FunctionCall) otto.Value {
		args := exportArgs(call.ArgumentList)
		if len(args) != 1 {
			return otto.NullValue()
		}
		qv, ok := toQuadValue(args[0])
		if !ok {
			return otto.NullValue()
		}
		return outObj(call, cmpOperator{op: op, val: qv})
	}
}

type cmpOperator struct {
	op  iterator.Operator
	val quad.Value
}

var defaultEnv = map[string]func(call otto.FunctionCall) otto.Value{
	"iri":   oneStringType(func(s string) quad.Value { return quad.IRI(s) }),
	"bnode": oneStringType(func(s string) quad.Value { return quad.BNode(s) }),
	"raw":   oneStringType(func(s string) quad.Value { return quad.Raw(s) }),
	"str":   oneStringType(func(s string) quad.Value { return quad.String(s) }),

	"lang": twoStringType(func(s, lang string) quad.Value {
		return quad.LangString{Value: quad.String(s), Lang: lang}
	}),
	"typed": twoStringType(func(s, typ string) quad.Value {
		return quad.TypedString{Value: quad.String(s), Type: quad.IRI(typ)}
	}),

	"lt":  cmpOpType(iterator.CompareLT),
	"lte": cmpOpType(iterator.CompareLTE),
	"gt":  cmpOpType(iterator.CompareGT),
	"gte": cmpOpType(iterator.CompareGTE),
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
	for name, val := range defaultEnv {
		env.Set(name, val)
	}
	return wk
}

func (wk *worker) wantShape() bool {
	return wk.shape != nil
}

func exportAsPath(args []otto.Value) (*pathObject, bool) {
	if len(args) == 0 {
		return nil, true
	}
	o, err := args[0].Export()
	if err != nil { // TODO(dennwc): throw js exception
		return nil, false
	}
	po, ok := o.(*pathObject)
	return po, ok
}

func unwrap(o interface{}) interface{} {
	switch v := o.(type) {
	case quadValue:
		o = v.v
	case *pathObject:
		o = v.path
	case []interface{}:
		for i := range v {
			v[i] = unwrap(v[i])
		}
	case map[string]interface{}:
		for k := range v {
			v[k] = unwrap(v[k])
		}
	}
	return o
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
			out = append(out, unwrap(o))
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

// quadValue is a wrapper to prevent otto from converting value to native JS type.
type quadValue struct {
	v quad.Value
}

func toQuadValue(o interface{}) (quad.Value, bool) {
	var qv quad.Value
	switch v := o.(type) {
	case quadValue:
		qv = v.v
	case quad.Value:
		qv = v
	case string:
		qv = quad.StringToValue(v)
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

func toQuadValues(objs []interface{}) ([]quad.Value, error) {
	if len(objs) == 0 {
		return nil, nil
	}
	vals := make([]quad.Value, 0, len(objs))
	for _, o := range objs {
		qv, ok := toQuadValue(o)
		if !ok {
			return nil, fmt.Errorf("unsupported type: %T", o)
		}
		vals = append(vals, qv)
	}
	return vals, nil
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
		if _, ok := via[i].(*path.Path); ok {
			// bypass
		} else if vp, ok := via[i].(*pathObject); ok {
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
