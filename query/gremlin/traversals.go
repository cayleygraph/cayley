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
	"github.com/robertkrimen/otto"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/path"
)

type pathObject struct {
	wk     *worker
	finals bool
	path   *path.Path
}

func (p *pathObject) clone(np *path.Path) *pathObject {
	return &pathObject{
		wk:     p.wk,
		finals: p.finals,
		path:   np,
	}
}
func (p *pathObject) buildIteratorTree() graph.Iterator {
	if p.path == nil {
		return iterator.NewNull()
	}
	return p.path.BuildIteratorOn(p.wk.qs)
}
func (p *pathObject) Is(call otto.FunctionCall) otto.Value {
	args := toQuadValues(exportArgs(call.ArgumentList))
	np := p.path.Is(args...)
	return outObj(call, p.clone(np))
}
func (p *pathObject) inout(call otto.FunctionCall, in bool) otto.Value {
	preds, tags, ok := toViaData(exportArgs(call.ArgumentList))
	if !ok {
		return otto.NullValue()
	}
	var np *path.Path
	if in {
		np = p.path.InWithTags(tags, preds...)
	} else {
		np = p.path.OutWithTags(tags, preds...)
	}
	return outObj(call, p.clone(np))
}
func (p *pathObject) In(call otto.FunctionCall) otto.Value {
	return p.inout(call, true)
}
func (p *pathObject) Out(call otto.FunctionCall) otto.Value {
	return p.inout(call, false)
}
func (p *pathObject) Both(call otto.FunctionCall) otto.Value {
	preds, _, ok := toViaData(exportArgs(call.ArgumentList))
	if !ok {
		return otto.NullValue()
	}
	np := p.path.Both(preds...)
	return outObj(call, p.clone(np))
}
func (p *pathObject) follow(call otto.FunctionCall, rev bool) otto.Value {
	ep := exportAsPath(call.ArgumentList)
	var np *path.Path
	if rev {
		np = p.path.FollowReverse(ep.path)
	} else {
		np = p.path.Follow(ep.path)
	}
	return outObj(call, p.clone(np))
}
func (p *pathObject) Follow(call otto.FunctionCall) otto.Value {
	return p.follow(call, false)
}
func (p *pathObject) FollowR(call otto.FunctionCall) otto.Value {
	return p.follow(call, true)
}
func (p *pathObject) And(call otto.FunctionCall) otto.Value {
	ep := exportAsPath(call.ArgumentList)
	np := p.path.And(ep.path)
	return outObj(call, p.clone(np))
}
func (p *pathObject) Intersect(call otto.FunctionCall) otto.Value {
	return p.And(call)
}
func (p *pathObject) Union(call otto.FunctionCall) otto.Value {
	return p.Or(call)
}
func (p *pathObject) Or(call otto.FunctionCall) otto.Value {
	ep := exportAsPath(call.ArgumentList)
	np := p.path.Or(ep.path)
	return outObj(call, p.clone(np))
}
func (p *pathObject) Back(call otto.FunctionCall) otto.Value {
	args := toStrings(exportArgs(call.ArgumentList))
	if len(args) != 1 {
		return otto.NullValue()
	}
	np := p.path.Back(args[0])
	return outObj(call, p.clone(np))
}
func (p *pathObject) Tag(call otto.FunctionCall) otto.Value {
	args := toStrings(exportArgs(call.ArgumentList))
	np := p.path.Tag(args...)
	return outObj(call, p.clone(np))
}
func (p *pathObject) As(call otto.FunctionCall) otto.Value {
	return p.Tag(call)
}
func (p *pathObject) Has(call otto.FunctionCall) otto.Value {
	args := exportArgs(call.ArgumentList)
	if len(args) == 0 {
		return otto.NullValue()
	}
	via := args[0]
	if vp, ok := via.(*pathObject); ok {
		via = vp.path
	}
	qv := toQuadValues(args[1:])
	np := p.path.Has(via, qv...)
	return outObj(call, p.clone(np))
}
func (p *pathObject) save(call otto.FunctionCall, rev bool) otto.Value {
	args := exportArgs(call.ArgumentList)
	if len(args) > 2 || len(args) == 0 {
		return otto.NullValue()
	}
	tag := args[0]
	if len(args) == 2 {
		tag = args[1]
	}
	via := args[0]
	if vp, ok := via.(*pathObject); ok {
		via = vp.path
	}
	var np *path.Path
	if rev {
		np = p.path.SaveReverse(via, tag.(string))
	} else {
		np = p.path.Save(via, tag.(string))
	}
	return outObj(call, p.clone(np))
}
func (p *pathObject) Save(call otto.FunctionCall) otto.Value {
	return p.save(call, false)
}
func (p *pathObject) SaveR(call otto.FunctionCall) otto.Value {
	return p.save(call, true)
}
func (p *pathObject) Except(call otto.FunctionCall) otto.Value {
	ep := exportAsPath(call.ArgumentList)
	np := p.path.Except(ep.path)
	return outObj(call, p.clone(np))
}
func (p *pathObject) Difference(call otto.FunctionCall) otto.Value {
	return p.Except(call)
}
func (p *pathObject) InPredicates(call otto.FunctionCall) otto.Value {
	np := p.path.InPredicates()
	return outObj(call, p.clone(np))
}
func (p *pathObject) OutPredicates(call otto.FunctionCall) otto.Value {
	np := p.path.OutPredicates()
	return outObj(call, p.clone(np))
}
func (p *pathObject) LabelContext(call otto.FunctionCall) otto.Value {
	labels, tags, ok := toViaData(exportArgs(call.ArgumentList))
	if !ok {
		return otto.NullValue()
	}
	np := p.path.LabelContextWithTags(tags, labels...)
	return outObj(call, p.clone(np))
}
func (p *pathObject) Filter(call otto.FunctionCall) otto.Value {
	args := exportArgs(call.ArgumentList)
	if len(args) == 0 {
		return otto.NullValue()
	}
	np := p.path
	for _, arg := range args {
		op, ok := arg.(cmpOperator)
		if !ok {
			return otto.NullValue()
		}
		np = np.Filter(op.op, op.val)
	}
	return outObj(call, p.clone(np))
}
