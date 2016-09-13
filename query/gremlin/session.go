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

import (
	"fmt"
	"sort"

	"github.com/robertkrimen/otto"
	"golang.org/x/net/context"
	// Provide underscore JS library.
	_ "github.com/robertkrimen/otto/underscore"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
)

const Name = "gremlin"

func init() {
	query.RegisterLanguage(query.Language{
		Name: Name,
		Session: func(qs graph.QuadStore) query.Session {
			return NewSession(qs, false)
		},
		HTTP: func(qs graph.QuadStore) query.HTTP {
			return NewSession(qs, false)
		},
		REPL: func(qs graph.QuadStore) query.REPLSession {
			return NewSession(qs, true)
		},
	})
}

type errKilled struct {
	Err error
}

func (e errKilled) Error() string { return e.Err.Error() }

type Session struct {
	qs graph.QuadStore

	wk      *worker
	script  *otto.Script
	persist *otto.Otto

	kill chan struct{}

	dataOutput []interface{}

	err error
}

func NewSession(qs graph.QuadStore, persist bool) *Session {
	g := Session{
		qs: qs,
		wk: newWorker(qs),
	}
	if persist {
		g.persist = g.wk.env
	}
	return &g
}

type Result struct {
	metaresult    bool
	err           error
	val           interface{}
	actualResults map[string]graph.Value
}

func (r *Result) Err() error { return r.err }
func (r *Result) Result() interface{} {
	if r.metaresult {
		return nil
	} else if r.val == nil {
		if r.actualResults == nil {
			return nil
		}
		return r.actualResults
	} else {
		return r.val
	}
}

func (s *Session) ShapeOf(query string) (interface{}, error) {
	// TODO(kortschak) It would be nice to be able
	// to return an error for bad queries here.
	s.wk.shape = make(map[string]interface{})
	s.wk.env.Run(query)
	out := s.wk.shape
	s.wk.shape = nil
	return out, nil
}

func (s *Session) runUnsafe(ctx context.Context, input interface{}) (_ otto.Value, gerr error) {
	wk := s.wk
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(errKilled); ok {
				s.err = e.Err
				wk.env = s.persist
				if gerr == nil {
					gerr = e.Err
				}
				return
			} else if err, ok := r.(error); ok {
				gerr = err
			} else {
				gerr = fmt.Errorf("recovered: %v", err)
			}
		}
	}()

	// Use buffered chan to prevent blocking.
	wk.env.Interrupt = make(chan func(), 1)
	s.kill = make(chan struct{})
	wk.kill = s.kill

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-done:
		case <-ctx.Done(): // timeout or cancelled
			close(s.kill)
			wk.Lock()
			if wk.env != nil {
				wk.env.Interrupt <- func() {
					panic(errKilled{ctx.Err()})
				}
			}
			wk.Unlock()
		}
	}()

	wk.Lock()
	env := wk.env
	wk.Unlock()
	return env.Run(input)
}

func (s *Session) Execute(ctx context.Context, input string, out chan query.Result, _ int) {
	// FIXME: use limit
	defer close(out)
	s.err = nil
	s.wk.results = out
	var (
		err   error
		value otto.Value
	)
	if s.script == nil {
		value, err = s.runUnsafe(ctx, input)
	} else {
		value, err = s.runUnsafe(ctx, s.script)
	}
	out <- &Result{
		metaresult: true,
		err:        err,
		val:        exportArgs([]otto.Value{value})[0],
	}
	s.wk.results = nil
	s.script = nil
	s.wk.Lock()
	s.wk.env = s.persist
	s.wk.Unlock()
}

func (s *Session) FormatREPL(result query.Result) string {
	data, ok := result.(*Result)
	if !ok {
		return fmt.Sprintf("Error: unexpected result type: %T\n", result)
	}
	if data.metaresult {
		if data.err != nil {
			return fmt.Sprintf("Error: %v\n", data.err)
		}
		if data.val != nil {
			s := data.val
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
	if data.val == nil {
		tags := data.actualResults
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
		switch export := data.val.(type) {
		case map[string]string:
			for k, v := range export {
				out += fmt.Sprintf("%s : %s\n", k, v)
			}
		case map[string]interface{}:
			for k, v := range export {
				out += fmt.Sprintf("%s : %v\n", k, v)
			}
		default:
			out += fmt.Sprintf("%s\n", data.val)
		}
	}
	return out
}

// Web stuff
func (s *Session) Collate(result query.Result) {
	data, ok := result.(*Result)
	if !ok {
		clog.Errorf("unexpected result type: %T", result)
		return
	} else if data.metaresult {
		return
	}
	if data.val != nil {
		s.dataOutput = append(s.dataOutput, data.val)
		return
	}
	obj := make(map[string]interface{})
	tags := data.actualResults
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
