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

func (p *pathObject) new(np *path.Path) *pathObject {
	return &pathObject{
		wk:     p.wk,
		finals: p.finals,
		path:   np,
	}
}
func (p *pathObject) clonePath() *path.Path {
	np := p.path.Clone()
	// most likely path will be continued, so we'll put non-capped stack slice
	// into new path object instead of preserving it in an old one
	p.path, np = np, p.path
	return np
}
func (p *pathObject) buildIteratorTree() graph.Iterator {
	if p.path == nil {
		return iterator.NewNull()
	}
	return p.path.BuildIteratorOn(p.wk.qs)
}
func (p *pathObject) Is(call otto.FunctionCall) otto.Value {
	args, err := toQuadValues(exportArgs(call.ArgumentList))
	if err != nil {
		//TODO(dennwc): pass error to caller
		return otto.NullValue()
	}
	np := p.clonePath().Is(args...)
	return outObj(call, p.new(np))
}
func (p *pathObject) inout(call otto.FunctionCall, in bool) otto.Value {
	preds, tags, ok := toViaData(exportArgs(call.ArgumentList))
	if !ok {
		return otto.NullValue()
	}
	np := p.clonePath()
	if in {
		np = np.InWithTags(tags, preds...)
	} else {
		np = np.OutWithTags(tags, preds...)
	}
	return outObj(call, p.new(np))
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
	np := p.clonePath().Both(preds...)
	return outObj(call, p.new(np))
}
func (p *pathObject) follow(call otto.FunctionCall, rev bool) otto.Value {
	ep, ok := exportAsPath(call.ArgumentList)
	if !ok {
		return otto.NullValue()
	}
	np := p.clonePath()
	if rev {
		np = np.FollowReverse(ep.path)
	} else {
		np = np.Follow(ep.path)
	}
	return outObj(call, p.new(np))
}
func (p *pathObject) Follow(call otto.FunctionCall) otto.Value {
	return p.follow(call, false)
}
func (p *pathObject) FollowR(call otto.FunctionCall) otto.Value {
	return p.follow(call, true)
}
func (p *pathObject) And(call otto.FunctionCall) otto.Value {
	ep, ok := exportAsPath(call.ArgumentList)
	if !ok {
		return otto.NullValue()
	}
	np := p.clonePath().And(ep.path)
	return outObj(call, p.new(np))
}
func (p *pathObject) Intersect(call otto.FunctionCall) otto.Value {
	return p.And(call)
}
func (p *pathObject) Union(call otto.FunctionCall) otto.Value {
	return p.Or(call)
}
func (p *pathObject) Or(call otto.FunctionCall) otto.Value {
	ep, ok := exportAsPath(call.ArgumentList)
	if !ok {
		return otto.NullValue()
	}
	np := p.clonePath().Or(ep.path)
	return outObj(call, p.new(np))
}
func (p *pathObject) Back(call otto.FunctionCall) otto.Value {
	args := toStrings(exportArgs(call.ArgumentList))
	if len(args) != 1 {
		return otto.NullValue()
	}
	np := p.clonePath().Back(args[0])
	return outObj(call, p.new(np))
}
func (p *pathObject) Tag(call otto.FunctionCall) otto.Value {
	args := toStrings(exportArgs(call.ArgumentList))
	np := p.clonePath().Tag(args...)
	return outObj(call, p.new(np))
}
func (p *pathObject) As(call otto.FunctionCall) otto.Value {
	return p.Tag(call)
}
func (p *pathObject) Has(call otto.FunctionCall) otto.Value {
	return p.has(call, false)
}
func (p *pathObject) HasR(call otto.FunctionCall) otto.Value {
	return p.has(call, true)
}
func (p *pathObject) has(call otto.FunctionCall, rev bool) otto.Value {
	args := exportArgs(call.ArgumentList)
	if len(args) == 0 {
		return otto.NullValue()
	}
	via := args[0]
	if vp, ok := via.(*pathObject); ok {
		via = vp.path
	} else {
		via, ok = toQuadValue(via)
		if !ok {
			return otto.NullValue()
		}
	}
	qv, err := toQuadValues(args[1:])
	if err != nil {
		//TODO(dennwc): pass error to caller
		return otto.NullValue()
	}
	np := p.clonePath()
	if rev {
		np = np.HasReverse(via, qv...)
	} else {
		np = np.Has(via, qv...)
	}
	return outObj(call, p.new(np))
}
func (p *pathObject) save(call otto.FunctionCall, rev bool) otto.Value {
	args := exportArgs(call.ArgumentList)
	if len(args) > 2 || len(args) == 0 {
		return otto.NullValue()
	}
	vtag := args[0]
	if len(args) == 2 {
		vtag = args[1]
	}
	tag, ok := vtag.(string)
	if !ok {
		return otto.NullValue()
	}
	via := args[0]
	if vp, ok := via.(*pathObject); ok {
		via = vp.path
	} else {
		via, ok = toQuadValue(via)
		if !ok {
			return otto.NullValue()
		}
	}
	np := p.clonePath()
	if rev {
		np = np.SaveReverse(via, tag)
	} else {
		np = np.Save(via, tag)
	}
	return outObj(call, p.new(np))
}
func (p *pathObject) Save(call otto.FunctionCall) otto.Value {
	return p.save(call, false)
}
func (p *pathObject) SaveR(call otto.FunctionCall) otto.Value {
	return p.save(call, true)
}
func (p *pathObject) Except(call otto.FunctionCall) otto.Value {
	ep, ok := exportAsPath(call.ArgumentList)
	if !ok {
		return otto.NullValue()
	}
	np := p.clonePath().Except(ep.path)
	return outObj(call, p.new(np))
}
func (p *pathObject) Unique(call otto.FunctionCall) otto.Value {
	if len(call.ArgumentList) != 0 {
		return otto.NullValue()
	}
	np := p.clonePath().Unique()
	return outObj(call, p.new(np))
}
func (p *pathObject) Difference(call otto.FunctionCall) otto.Value {
	return p.Except(call)
}
func (p *pathObject) InPredicates(call otto.FunctionCall) otto.Value {
	np := p.clonePath().InPredicates()
	return outObj(call, p.new(np))
}
func (p *pathObject) OutPredicates(call otto.FunctionCall) otto.Value {
	np := p.clonePath().OutPredicates()
	return outObj(call, p.new(np))
}
func (p *pathObject) LabelContext(call otto.FunctionCall) otto.Value {
	labels, tags, ok := toViaData(exportArgs(call.ArgumentList))
	if !ok {
		return otto.NullValue()
	}
	np := p.clonePath().LabelContextWithTags(tags, labels...)
	return outObj(call, p.new(np))
}
func (p *pathObject) Filter(call otto.FunctionCall) otto.Value {
	args := exportArgs(call.ArgumentList)
	if len(args) == 0 {
		return otto.NullValue()
	}
	np := p.clonePath()
	for _, arg := range args {
		op, ok := arg.(cmpOperator)
		if !ok {
			return otto.NullValue()
		}
		var err error
		np, err = op.apply(call, np)
		if err != nil {
			return throwErr(call, err)
		}
	}
	return outObj(call, p.new(np))
}
func (p *pathObject) Limit(call otto.FunctionCall) otto.Value {
	args := exportArgs(call.ArgumentList)
	np := p.clonePath().Limit(int64(toInt(args[0])))
	return outObj(call, p.new(np))
}
func (p *pathObject) Skip(call otto.FunctionCall) otto.Value {
	args := exportArgs(call.ArgumentList)
	np := p.clonePath().Skip(int64(toInt(args[0])))
	return outObj(call, p.new(np))
}
func (p *pathObject) Count(call otto.FunctionCall) otto.Value {
	if len(call.ArgumentList) != 0 {
		return otto.NullValue()
	}
	np := p.clonePath().Count()
	return outObj(call, p.new(np))
}
