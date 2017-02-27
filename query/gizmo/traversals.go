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

// Adds special traversal functions to JS Gizmo objects. Most of these just build the chain of objects, and won't often need the session.

import (
	"fmt"

	"github.com/dop251/goja"

	"github.com/codelingo/cayley/graph"
	"github.com/codelingo/cayley/graph/iterator"
	"github.com/codelingo/cayley/graph/path"
)

type pathObject struct {
	s      *Session
	finals bool
	path   *path.Path
}

func (p *pathObject) new(np *path.Path) *pathObject {
	return &pathObject{
		s:      p.s,
		finals: p.finals,
		path:   np,
	}
}

func (p *pathObject) newVal(np *path.Path) goja.Value {
	return p.s.vm.ToValue(p.new(np))
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
	return p.path.BuildIteratorOn(p.s.qs)
}
func (p *pathObject) Is(call goja.FunctionCall) goja.Value {
	args, err := toQuadValues(exportArgs(call.Arguments))
	if err != nil {
		return throwErr(p.s.vm, err)
	}
	np := p.clonePath().Is(args...)
	return p.newVal(np)
}
func (p *pathObject) inout(call goja.FunctionCall, in bool) goja.Value {
	preds, tags, ok := toViaData(exportArgs(call.Arguments))
	if !ok {
		return throwErr(p.s.vm, errNoVia)
	}
	np := p.clonePath()
	if in {
		np = np.InWithTags(tags, preds...)
	} else {
		np = np.OutWithTags(tags, preds...)
	}
	return p.newVal(np)
}
func (p *pathObject) In(call goja.FunctionCall) goja.Value {
	return p.inout(call, true)
}
func (p *pathObject) Out(call goja.FunctionCall) goja.Value {
	return p.inout(call, false)
}
func (p *pathObject) Both(call goja.FunctionCall) goja.Value {
	preds, tags, ok := toViaData(exportArgs(call.Arguments))
	if !ok {
		return throwErr(p.s.vm, errNoVia)
	}
	np := p.clonePath().BothWithTags(tags, preds...)
	return p.newVal(np)
}
func (p *pathObject) follow(ep *pathObject, rev bool) *pathObject {
	np := p.clonePath()
	if rev {
		np = np.FollowReverse(ep.path)
	} else {
		np = np.Follow(ep.path)
	}
	return p.new(np)
}
func (p *pathObject) Follow(ep *pathObject) *pathObject {
	return p.follow(ep, false)
}
func (p *pathObject) FollowR(ep *pathObject) *pathObject {
	return p.follow(ep, true)
}
func (p *pathObject) And(ep *pathObject) *pathObject {
	np := p.clonePath().And(ep.path)
	return p.new(np)
}
func (p *pathObject) Intersect(ep *pathObject) *pathObject {
	return p.And(ep)
}
func (p *pathObject) Union(ep *pathObject) *pathObject {
	return p.Or(ep)
}
func (p *pathObject) Or(ep *pathObject) *pathObject {
	np := p.clonePath().Or(ep.path)
	return p.new(np)
}
func (p *pathObject) Back(tag string) *pathObject {
	np := p.clonePath().Back(tag)
	return p.new(np)
}
func (p *pathObject) Tag(tags ...string) *pathObject {
	np := p.clonePath().Tag(tags...)
	return p.new(np)
}
func (p *pathObject) As(tags ...string) *pathObject {
	return p.Tag(tags...)
}
func (p *pathObject) Has(call goja.FunctionCall) goja.Value {
	return p.has(call, false)
}
func (p *pathObject) HasR(call goja.FunctionCall) goja.Value {
	return p.has(call, true)
}
func (p *pathObject) has(call goja.FunctionCall, rev bool) goja.Value {
	args := exportArgs(call.Arguments)
	if len(args) == 0 {
		return throwErr(p.s.vm, errArgCount{Got: len(args)})
	}
	via := args[0]
	if vp, ok := via.(*pathObject); ok {
		via = vp.path
	} else {
		var err error
		via, err = toQuadValue(via)
		if err != nil {
			return throwErr(p.s.vm, err)
		}
	}
	qv, err := toQuadValues(args[1:])
	if err != nil {
		return throwErr(p.s.vm, err)
	}
	np := p.clonePath()
	if rev {
		np = np.HasReverse(via, qv...)
	} else {
		np = np.Has(via, qv...)
	}
	return p.newVal(np)
}
func (p *pathObject) save(call goja.FunctionCall, rev bool) goja.Value {
	args := exportArgs(call.Arguments)
	if len(args) > 2 || len(args) == 0 {
		return throwErr(p.s.vm, errArgCount{Got: len(args)})
	}
	vtag := args[0]
	if len(args) == 2 {
		vtag = args[1]
	}
	tag, ok := vtag.(string)
	if !ok {
		return throwErr(p.s.vm, fmt.Errorf("expected string, got: %T", vtag))
	}
	via := args[0]
	if vp, ok := via.(*pathObject); ok {
		via = vp.path
	} else {
		var err error
		via, err = toQuadValue(via)
		if err != nil {
			return throwErr(p.s.vm, err)
		}
	}
	np := p.clonePath()
	if rev {
		np = np.SaveReverse(via, tag)
	} else {
		np = np.Save(via, tag)
	}
	return p.newVal(np)
}
func (p *pathObject) Save(call goja.FunctionCall) goja.Value {
	return p.save(call, false)
}
func (p *pathObject) SaveR(call goja.FunctionCall) goja.Value {
	return p.save(call, true)
}
func (p *pathObject) Except(ep *pathObject) *pathObject {
	np := p.clonePath().Except(ep.path)
	return p.new(np)
}
func (p *pathObject) Unique() *pathObject {
	np := p.clonePath().Unique()
	return p.new(np)
}
func (p *pathObject) Difference(ep *pathObject) *pathObject {
	return p.Except(ep)
}
func (p *pathObject) InPredicates() *pathObject {
	np := p.clonePath().InPredicates()
	return p.new(np)
}
func (p *pathObject) OutPredicates() *pathObject {
	np := p.clonePath().OutPredicates()
	return p.new(np)
}
func (p *pathObject) LabelContext(call goja.FunctionCall) goja.Value {
	labels, tags, ok := toViaData(exportArgs(call.Arguments))
	if !ok {
		return throwErr(p.s.vm, errNoVia)
	}
	np := p.clonePath().LabelContextWithTags(tags, labels...)
	return p.newVal(np)
}
func (p *pathObject) Filter(args ...cmpOperator) (*pathObject, error) {
	if len(args) == 0 {
		return nil, errArgCount{Got: len(args)}
	}
	np := p.clonePath()
	for _, op := range args {
		var err error
		np, err = op.apply(np)
		if err != nil {
			return nil, err
		}
	}
	return p.new(np), nil
}
func (p *pathObject) Limit(n int) *pathObject {
	np := p.clonePath().Limit(int64(n))
	return p.new(np)
}
func (p *pathObject) Skip(n int) *pathObject {
	np := p.clonePath().Skip(int64(n))
	return p.new(np)
}
func (p *pathObject) Count() *pathObject {
	np := p.clonePath().Count()
	return p.new(np)
}
