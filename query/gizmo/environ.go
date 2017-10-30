// Copyright 2017 The Cayley Authors. All rights reserved.
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

package gizmo

// Builds a new Gizmo environment pointing at a session.

import (
	"fmt"
	"regexp"
	"time"

	"github.com/dop251/goja"

	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/path"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/schema"
	"github.com/cayleygraph/cayley/voc"
)

// graphObject is a root graph object.
//
// Name: `graph`, Alias: `g`
//
// This is the only special object in the environment, generates the query objects.
// Under the hood, they're simple objects that get compiled to a Go iterator tree when executed.
type graphObject struct {
	s *Session
}

// Uri creates an IRI values from a given string.
func (g *graphObject) Uri(s string) quad.IRI {
	return quad.IRI(g.s.ns.FullIRI(s))
}

// AddNamespace associates prefix with a given IRI namespace.
func (g *graphObject) AddNamespace(pref, ns string) {
	g.s.ns.Register(voc.Namespace{Prefix: pref + ":", Full: ns})
}

// AddDefaultNamespaces register all default namespaces for automatic IRI resolution.
func (g *graphObject) AddDefaultNamespaces() {
	voc.CloneTo(&g.s.ns)
}

// LoadNamespaces loads all namespaces saved to graph.
func (g *graphObject) LoadNamespaces() error {
	return schema.LoadNamespaces(g.s.ctx, g.s.qs, &g.s.ns)
}

// V is a shorthand for Vertex.
func (g *graphObject) V(call goja.FunctionCall) goja.Value {
	return g.Vertex(call)
}

// Vertex starts a query path at the given vertex/vertices. No ids means "all vertices".
// Signature: ([nodeId],[nodeId]...)
//
// Arguments:
//
// * `nodeId` (Optional): A string or list of strings representing the starting vertices.
//
// Returns: Path object
func (g *graphObject) Vertex(call goja.FunctionCall) goja.Value {
	qv, err := toQuadValues(exportArgs(call.Arguments))
	if err != nil {
		return throwErr(g.s.vm, err)
	}
	return g.s.vm.ToValue(&pathObject{
		s:      g.s,
		finals: true,
		path:   path.StartMorphism(qv...),
	})
}

// M is a shorthand for Morphism.
func (g *graphObject) M() *pathObject {
	return g.Morphism()
}

// Morphism creates a morphism path object. Unqueryable on it's own, defines one end of the path.
// Saving these to variables with
//
//	// javascript
//	var shorterPath = graph.Morphism().Out("foo").Out("bar")
//
// is the common use case. See also: path.Follow(), path.FollowR().
func (g *graphObject) Morphism() *pathObject {
	return &pathObject{
		s:    g.s,
		path: path.StartMorphism(),
	}
}

// Emit adds data programmatically to the JSON result list. Can be any JSON type.
//
//	// javascript
//	g.Emit({name:"bob"}) // push {"name":"bob"} as a result
func (g *graphObject) Emit(call goja.FunctionCall) goja.Value {
	value := call.Argument(0)
	if !goja.IsNull(value) && !goja.IsUndefined(value) {
		val := exportArgs([]goja.Value{value})[0]
		if val != nil {
			g.s.send(nil, &Result{Val: val})
		}
	}
	return goja.Null()
}

func oneStringType(fnc func(s string) quad.Value) func(vm *goja.Runtime, call goja.FunctionCall) goja.Value {
	return func(vm *goja.Runtime, call goja.FunctionCall) goja.Value {
		args := toStrings(exportArgs(call.Arguments))
		if len(args) != 1 {
			return throwErr(vm, errArgCount2{Expected: 1, Got: len(args)})
		}
		return vm.ToValue(fnc(args[0]))
	}
}

func twoStringType(fnc func(s1, s2 string) quad.Value) func(vm *goja.Runtime, call goja.FunctionCall) goja.Value {
	return func(vm *goja.Runtime, call goja.FunctionCall) goja.Value {
		args := toStrings(exportArgs(call.Arguments))
		if len(args) != 2 {
			return throwErr(vm, errArgCount2{Expected: 2, Got: len(args)})
		}
		return vm.ToValue(fnc(args[0], args[1]))
	}
}

func cmpOpType(op iterator.Operator) func(vm *goja.Runtime, call goja.FunctionCall) goja.Value {
	return func(vm *goja.Runtime, call goja.FunctionCall) goja.Value {
		args := exportArgs(call.Arguments)
		if len(args) != 1 {
			return throwErr(vm, errArgCount2{Expected: 1, Got: len(args)})
		}
		qv, err := toQuadValue(args[0])
		if err != nil {
			return throwErr(vm, err)
		}
		return vm.ToValue(cmpOperator{op: op, val: qv})
	}
}

func cmpRegexp(vm *goja.Runtime, call goja.FunctionCall) goja.Value {
	args := exportArgs(call.Arguments)
	if len(args) != 1 && len(args) != 2 {
		return throwErr(vm, errArgCount2{Expected: 1, Got: len(args)})
	}
	v, err := toQuadValue(args[0])
	if err != nil {
		return throwErr(vm, err)
	}
	allowRefs := false
	if len(args) > 1 {
		b, ok := args[1].(bool)
		if !ok {
			return throwErr(vm, fmt.Errorf("expected bool as second argument"))
		}
		allowRefs = b
	}
	switch vt := v.(type) {
	case quad.String:
		if allowRefs {
			v = quad.IRI(string(vt))
		}
	case quad.IRI:
		if !allowRefs {
			return throwErr(vm, errRegexpOnIRI)
		}
	case quad.BNode:
		if !allowRefs {
			return throwErr(vm, errRegexpOnIRI)
		}
	default:
		return throwErr(vm, fmt.Errorf("regexp: unsupported type: %T", v))
	}
	return vm.ToValue(cmpOperator{regex: true, val: v})
}

type cmpOperator struct {
	op    iterator.Operator
	val   quad.Value
	regex bool
}

func (op cmpOperator) apply(p *path.Path) (*path.Path, error) {
	if !op.regex {
		p = p.Filter(op.op, op.val)
		return p, nil
	}
	var (
		s    string
		refs bool
	)
	switch v := op.val.(type) {
	case quad.String:
		s = string(v)
	case quad.IRI:
		s, refs = string(v), true
	case quad.BNode:
		s, refs = string(v), true
	default:
		return p, fmt.Errorf("regexp from non-string value: %T", op.val)
	}
	re, err := regexp.Compile(string(s))
	if err != nil {
		return p, err
	}
	if refs {
		p = p.RegexWithRefs(re)
	} else {
		p = p.Regex(re)
	}
	return p, nil
}

var defaultEnv = map[string]func(vm *goja.Runtime, call goja.FunctionCall) goja.Value{
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

	"lt":    cmpOpType(iterator.CompareLT),
	"lte":   cmpOpType(iterator.CompareLTE),
	"gt":    cmpOpType(iterator.CompareGT),
	"gte":   cmpOpType(iterator.CompareGTE),
	"regex": cmpRegexp,
}

func unwrap(o interface{}) interface{} {
	switch v := o.(type) {
	case *pathObject:
		o = v.path
	case []interface{}:
		for i, val := range v {
			v[i] = unwrap(val)
		}
	case map[string]interface{}:
		for k, val := range v {
			v[k] = unwrap(val)
		}
	}
	return o
}

func exportArgs(args []goja.Value) []interface{} {
	if len(args) == 0 {
		return nil
	}
	out := make([]interface{}, 0, len(args))
	for _, a := range args {
		o := a.Export()
		out = append(out, unwrap(o))
	}
	return out
}

func toInt(o interface{}) (int, bool) {
	switch v := o.(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	default:
		return 0, false
	}
}

func toQuadValue(o interface{}) (quad.Value, error) {
	var qv quad.Value
	switch v := o.(type) {
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
		return nil, errNotQuadValue{Val: o}
	}
	return qv, nil
}

func toQuadValues(objs []interface{}) ([]quad.Value, error) {
	if len(objs) == 0 {
		return nil, nil
	}
	vals := make([]quad.Value, 0, len(objs))
	for _, o := range objs {
		qv, err := toQuadValue(o)
		if err != nil {
			return nil, err
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
		} else if v, ok := via[0].([]string); ok {
			arr := make([]interface{}, 0, len(v))
			for _, s := range v {
				arr = append(arr, s)
			}
			return toVia(arr)
		}
	}
	for i := range via {
		if _, ok := via[i].(*path.Path); ok {
			// bypass
		} else if vp, ok := via[i].(*pathObject); ok {
			via[i] = vp.path
		} else if qv, err := toQuadValue(via[i]); err == nil {
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
	if len(objs) > 1 {
		tags = toStrings(objs[1:])
	}
	ok = true
	return
}

func toViaDepthData(objs []interface{}) (predicates []interface{}, maxDepth int, tags []string, ok bool) {
	if len(objs) != 0 {
		predicates = toVia([]interface{}{objs[0]})
	}
	if len(objs) > 1 {
		maxDepth, ok = toInt(objs[1])
		if ok {
			if len(objs) > 2 {
				tags = toStrings(objs[2:])
			}
		} else {
			tags = toStrings(objs[1:])
		}
	}
	ok = true
	return
}

func throwErr(vm *goja.Runtime, err error) goja.Value {
	panic(vm.ToValue(err))
}
