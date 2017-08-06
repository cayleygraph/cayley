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

import (
	"context"
	"fmt"
	"sort"

	"github.com/dop251/goja"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/voc"
)

const Name = "gizmo"

func init() {
	query.RegisterLanguage(query.Language{
		Name: Name,
		Session: func(qs graph.QuadStore) query.Session {
			return NewSession(qs)
		},
		HTTP: func(qs graph.QuadStore) query.HTTP {
			return NewSession(qs)
		},
		REPL: func(qs graph.QuadStore) query.REPLSession {
			return NewSession(qs)
		},
	})
}

func NewSession(qs graph.QuadStore) *Session {
	s := &Session{
		qs: qs, limit: -1,
	}
	if err := s.buildEnv(); err != nil {
		panic(err)
	}
	return s
}

type Session struct {
	qs graph.QuadStore
	vm *goja.Runtime
	ns voc.Namespaces

	last string
	p    *goja.Program

	out   chan query.Result
	ctx   context.Context
	limit int
	count int

	// used only to collate web results
	dataOutput []interface{}
	err        error
	shape      map[string]interface{}
}

func (s *Session) context() context.Context {
	return s.ctx
}

func (s *Session) buildEnv() error {
	if s.vm != nil {
		return nil
	}
	s.vm = goja.New()
	s.vm.Set("graph", &graphObject{s: s})
	s.vm.Set("g", s.vm.Get("graph"))
	for name, val := range defaultEnv {
		fnc := val
		s.vm.Set(name, func(call goja.FunctionCall) goja.Value {
			return fnc(s.vm, call)
		})
	}
	return nil
}

func (s *Session) tagsToValueMap(m map[string]graph.Value) map[string]interface{} {
	outputMap := make(map[string]interface{})
	for k, v := range m {
		if o := quadValueToNative(s.qs.NameOf(v)); o != nil {
			outputMap[k] = o
		}
	}
	if len(outputMap) == 0 {
		return nil
	}
	return outputMap
}
func (s *Session) runIteratorToArray(it graph.Iterator, limit int) ([]map[string]interface{}, error) {
	ctx := s.context()

	output := make([]map[string]interface{}, 0)
	err := graph.Iterate(ctx, it).Limit(limit).TagEach(func(tags map[string]graph.Value) {
		tm := s.tagsToValueMap(tags)
		if tm == nil {
			return
		}
		output = append(output, tm)
	})
	if err != nil {
		return nil, err
	}
	return output, nil
}

func (s *Session) runIteratorToArrayNoTags(it graph.Iterator, limit int) ([]interface{}, error) {
	ctx := s.context()

	output := make([]interface{}, 0)
	err := graph.Iterate(ctx, it).Paths(false).Limit(limit).EachValue(s.qs, func(v quad.Value) {
		if o := quadValueToNative(v); o != nil {
			output = append(output, o)
		}
	})
	if err != nil {
		return nil, err
	}
	return output, nil
}

func (s *Session) runIteratorWithCallback(it graph.Iterator, callback goja.Value, this goja.FunctionCall, limit int) error {
	fnc, ok := goja.AssertFunction(callback)
	if !ok {
		return fmt.Errorf("expected js callback function")
	}
	ctx, cancel := context.WithCancel(s.context())
	defer cancel()
	var gerr error
	err := graph.Iterate(ctx, it).Paths(true).Limit(limit).TagEach(func(tags map[string]graph.Value) {
		tm := s.tagsToValueMap(tags)
		if tm == nil {
			return
		}
		if _, err := fnc(this.This, s.vm.ToValue(tm)); err != nil {
			gerr = err
			cancel()
		}
	})
	if gerr != nil {
		err = gerr
	}
	return err
}

func (s *Session) send(ctx context.Context, r *Result) bool {
	if s.limit >= 0 && s.count >= s.limit {
		return false
	}
	if s.out == nil {
		return false
	}
	if ctx == nil {
		ctx = s.ctx
	}
	select {
	case s.out <- r:
	case <-ctx.Done():
		return false
	}
	s.count++
	return s.limit < 0 || s.count < s.limit
}

func (s *Session) runIterator(it graph.Iterator) error {
	if s.shape != nil {
		iterator.OutputQueryShapeForIterator(it, s.qs, s.shape)
		return nil
	}

	ctx, cancel := context.WithCancel(s.context())
	defer cancel()
	stop := false
	err := graph.Iterate(ctx, it).Paths(true).TagEach(func(tags map[string]graph.Value) {
		if !s.send(ctx, &Result{Tags: tags}) {
			cancel()
			stop = true
		}
	})
	if stop {
		err = nil
	}
	return err
}

func (s *Session) countResults(it graph.Iterator) (int64, error) {
	if s.shape != nil {
		iterator.OutputQueryShapeForIterator(it, s.qs, s.shape)
		return 0, nil
	}
	return graph.Iterate(s.context(), it).Paths(true).Count()
}

type Result struct {
	Meta bool
	Val  interface{}
	Tags map[string]graph.Value
}

func (r *Result) Result() interface{} {
	if r.Tags != nil {
		return r.Tags
	}
	return r.Val
}
func (r *Result) Err() error { return nil }

func (s *Session) run(qu string) (v goja.Value, err error) {
	var p *goja.Program
	if s.last == qu && s.last != "" {
		p = s.p
	} else {
		p, err = goja.Compile("", qu, false)
		if err != nil {
			return
		}
		s.last, s.p = qu, p
	}
	v, err = s.vm.RunProgram(p)
	if e, ok := err.(*goja.Exception); ok && e.Value() != nil {
		if er, ok := e.Value().Export().(error); ok {
			err = er
		}
	}
	return v, err
}
func (s *Session) Execute(ctx context.Context, qu string, out chan query.Result, limit int) {
	defer close(out)
	s.out = out
	s.limit = limit
	s.count = 0
	s.ctx = ctx
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			s.vm.Interrupt(ctx.Err())
		case <-done:
		}
	}()
	v, err := s.run(qu)
	if err != nil {
		select {
		case <-ctx.Done():
		case out <- query.ErrorResult(err):
		}
		return
	}
	if !goja.IsNull(v) && !goja.IsUndefined(v) {
		s.send(ctx, &Result{Meta: true, Val: v.Export()})
	}
}

func (s *Session) FormatREPL(result query.Result) string {
	if err := result.Err(); err != nil {
		return fmt.Sprintf("error: %v", err)
	}
	data, ok := result.(*Result)
	if !ok {
		return fmt.Sprintf("Error: unexpected result type: %T\n", result)
	}
	if data.Meta {
		if data.Val != nil {
			s := data.Val
			switch s.(type) {
			case *pathObject, *graphObject:
				s = "[internal Iterator]"
			}
			return fmt.Sprintln("=>", s)
		}
		return fmt.Sprintln("=>", nil)
	}
	var out string
	out = fmt.Sprintln("****")
	if data.Val == nil {
		tags := data.Tags
		tagKeys := make([]string, len(tags))
		i := 0
		for k := range tags {
			tagKeys[i] = k
			i++
		}
		sort.Strings(tagKeys)
		for _, k := range tagKeys {
			if k == "$_" {
				continue
			}
			out += fmt.Sprintf("%s : %s\n", k, quadValueToString(s.qs.NameOf(tags[k])))
		}
	} else {
		switch export := data.Val.(type) {
		case map[string]string:
			for k, v := range export {
				out += fmt.Sprintf("%s : %s\n", k, v)
			}
		case map[string]interface{}:
			for k, v := range export {
				out += fmt.Sprintf("%s : %v\n", k, v)
			}
		default:
			out += fmt.Sprintf("%s\n", data.Val)
		}
	}
	return out
}

// Web stuff

func (s *Session) ShapeOf(qu string) (interface{}, error) {
	s.shape = make(map[string]interface{})
	_, err := s.run(qu)
	out := s.shape
	s.shape = nil
	return out, err
}

func (s *Session) Collate(result query.Result) {
	if err := result.Err(); err != nil {
		s.err = err
		return
	}
	data, ok := result.(*Result)
	if !ok {
		clog.Errorf("unexpected result type: %T", result)
		return
	} else if data.Meta {
		return
	}
	if data.Val != nil {
		s.dataOutput = append(s.dataOutput, data.Val)
		return
	}
	obj := make(map[string]interface{})
	tags := data.Tags
	var tagKeys []string
	for k := range tags {
		tagKeys = append(tagKeys, k)
	}
	sort.Strings(tagKeys)
	for _, k := range tagKeys {
		if name := s.qs.NameOf(tags[k]); name != nil {
			obj[k] = quadValueToNative(name)
		} else {
			delete(obj, k)
		}
	}
	if len(obj) != 0 {
		s.dataOutput = append(s.dataOutput, obj)
	}
}

func (s *Session) Results() (interface{}, error) {
	defer s.Clear()
	if s.err != nil {
		return nil, s.err
	}
	return s.dataOutput, nil
}

func (s *Session) Clear() {
	s.dataOutput = nil
}
