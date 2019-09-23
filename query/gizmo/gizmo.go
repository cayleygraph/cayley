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
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/dop251/goja"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/schema"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/jsonld"
	"github.com/cayleygraph/quad/voc"
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
		ctx: context.Background(),
		sch: schema.NewConfig(),
		qs:  qs, limit: -1,
	}
	if err := s.buildEnv(); err != nil {
		panic(err)
	}
	return s
}

func lcFirst(str string) string {
	rune, size := utf8.DecodeRuneInString(str)
	return string(unicode.ToLower(rune)) + str[size:]
}

type fieldNameMapper struct{}

func (fieldNameMapper) FieldName(t reflect.Type, f reflect.StructField) string {
	return lcFirst(f.Name)
}

const constructMethodPrefix = "New"
const backwardsCompatibilityPrefix = "Capitalized"

func (fieldNameMapper) MethodName(t reflect.Type, m reflect.Method) string {
	if strings.HasPrefix(m.Name, backwardsCompatibilityPrefix) {
		return strings.TrimPrefix(m.Name, backwardsCompatibilityPrefix)
	}
	if strings.HasPrefix(m.Name, constructMethodPrefix) {
		return strings.TrimPrefix(m.Name, constructMethodPrefix)
	}
	return lcFirst(m.Name)
}

type Session struct {
	qs  graph.QuadStore
	vm  *goja.Runtime
	ns  voc.Namespaces
	sch *schema.Config
	col query.Collation

	last string
	p    *goja.Program

	out   chan *Result
	ctx   context.Context
	limit int
	count int

	err   error
	shape map[string]interface{}
}

func (s *Session) context() context.Context {
	return s.ctx
}

func (s *Session) buildEnv() error {
	if s.vm != nil {
		return nil
	}
	s.vm = goja.New()
	s.vm.SetFieldNameMapper(fieldNameMapper{})
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

func (s *Session) quadValueToNative(v quad.Value) interface{} {
	if v == nil {
		return nil
	}
	if s.col == query.JSONLD {
		return jsonld.FromValue(v)
	}
	out := v.Native()
	if nv, ok := out.(quad.Value); ok && v == nv {
		return quad.StringOf(v)
	}
	return out
}

func (s *Session) tagsToValueMap(m map[string]graph.Ref) map[string]interface{} {
	outputMap := make(map[string]interface{})
	for k, v := range m {
		if o := s.quadValueToNative(s.qs.NameOf(v)); o != nil {
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
	err := graph.Iterate(ctx, it).Limit(limit).TagEach(func(tags map[string]graph.Ref) {
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
		if o := s.quadValueToNative(v); o != nil {
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
	err := graph.Iterate(ctx, it).Paths(true).Limit(limit).TagEach(func(tags map[string]graph.Ref) {
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
	if s.limit > 0 && s.count >= s.limit {
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
	return s.limit <= 0 || s.count < s.limit
}

func (s *Session) runIterator(it graph.Iterator) error {
	if s.shape != nil {
		iterator.OutputQueryShapeForIterator(it, s.qs, s.shape)
		return nil
	}

	ctx, cancel := context.WithCancel(s.context())
	defer cancel()
	stop := false
	err := graph.Iterate(ctx, it).Paths(true).TagEach(func(tags map[string]graph.Ref) {
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
	Tags map[string]graph.Ref
}

func (r *Result) Result() interface{} {
	if r.Tags != nil {
		return r.Tags
	}
	return r.Val
}

func (s *Session) compile(qu string) error {
	var p *goja.Program
	if s.last == qu && s.last != "" {
		p = s.p
	} else {
		var err error
		p, err = goja.Compile("", qu, false)
		if err != nil {
			return err
		}
		s.last, s.p = qu, p
	}
	return nil
}

func (s *Session) run() (goja.Value, error) {
	v, err := s.vm.RunProgram(s.p)
	if e, ok := err.(*goja.Exception); ok && e.Value() != nil {
		if er, ok := e.Value().Export().(error); ok {
			err = er
		}
	}
	return v, err
}
func (s *Session) Execute(ctx context.Context, qu string, opt query.Options) (query.Iterator, error) {
	switch opt.Collation {
	case query.Raw, query.JSON, query.JSONLD, query.REPL:
	default:
		return nil, &query.ErrUnsupportedCollation{Collation: opt.Collation}
	}
	if err := s.compile(qu); err != nil {
		return nil, err
	}
	s.limit = opt.Limit
	s.count = 0
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.col = opt.Collation
	return &results{
		col: opt.Collation,
		s:   s,
		ctx: ctx, cancel: cancel,
	}, nil
}

type results struct {
	s      *Session
	col    query.Collation
	ctx    context.Context
	cancel func()

	running bool
	errc    chan error

	err error
	cur *Result
}

func (it *results) stop(err error) {
	it.cancel()
	if !it.running {
		return
	}
	it.s.vm.Interrupt(err)
	it.running = false
}

func (it *results) Next(ctx context.Context) bool {
	if it.errc == nil {
		it.s.out = make(chan *Result)
		it.errc = make(chan error, 1)
		it.running = true
		go func() {
			defer close(it.errc)
			v, err := it.s.run()
			if err != nil {
				it.errc <- err
				return
			}
			if !goja.IsNull(v) && !goja.IsUndefined(v) {
				it.s.send(it.ctx, &Result{Meta: true, Val: v.Export()})
			}
		}()
	}
	select {
	case r := <-it.s.out:
		it.cur = r
		return true
	case err := <-it.errc:
		if err != nil {
			it.err = err
		}
		return false
	case <-ctx.Done():
		it.err = ctx.Err()
		it.stop(it.err)
		return false
	}
}

func (it *results) Result() interface{} {
	if it.cur == nil {
		return nil
	}
	switch it.col {
	case query.Raw:
		return it.cur
	case query.JSON, query.JSONLD:
		return it.jsonResult()
	case query.REPL:
		return it.replResult()
	}
	return nil
}

func (it *results) jsonResult() interface{} {
	data := it.cur
	if data.Meta {
		return nil
	}
	if data.Val != nil {
		return data.Val
	}
	obj := make(map[string]interface{})
	tags := data.Tags
	var tagKeys []string
	for k := range tags {
		tagKeys = append(tagKeys, k)
	}
	sort.Strings(tagKeys)
	for _, k := range tagKeys {
		if name := it.s.qs.NameOf(tags[k]); name != nil {
			obj[k] = it.s.quadValueToNative(name)
		} else {
			delete(obj, k)
		}
	}
	return obj
}

func (it *results) replResult() interface{} {
	data := it.cur
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
			out += fmt.Sprintf("%s : %s\n", k, quadValueToString(it.s.qs.NameOf(tags[k])))
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

func (it *results) Err() error {
	return it.err
}

func (it *results) Close() error {
	it.stop(errors.New("iterator closed"))
	return nil
}

// Web stuff

func (s *Session) ShapeOf(qu string) (interface{}, error) {
	s.shape = make(map[string]interface{})
	err := s.compile(qu)
	if err != nil {
		return nil, err
	}
	_, err = s.run()
	out := s.shape
	s.shape = nil
	return out, err
}
