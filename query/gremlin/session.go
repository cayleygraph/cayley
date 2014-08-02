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
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/robertkrimen/otto"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/query"
)

var ErrKillTimeout = errors.New("query timed out")

type Session struct {
	ts         graph.TripleStore
	results    chan interface{}
	env        *otto.Otto
	envLock    sync.Mutex
	debug      bool
	limit      int
	count      int
	dataOutput []interface{}
	wantShape  bool
	shape      map[string]interface{}
	err        error
	script     *otto.Script
	kill       chan struct{}
	timeoutSec time.Duration
	emptyEnv   *otto.Otto
}

func NewSession(inputTripleStore graph.TripleStore, timeoutSec int, persist bool) *Session {
	g := Session{
		ts:         inputTripleStore,
		limit:      -1,
		timeoutSec: time.Duration(timeoutSec),
	}
	g.env = BuildEnviron(&g)
	if persist {
		g.emptyEnv = g.env
	}
	return &g
}

type GremlinResult struct {
	metaresult    bool
	err           error
	val           *otto.Value
	actualResults *map[string]graph.Value
}

func (s *Session) ToggleDebug() {
	s.debug = !s.debug
}

func (s *Session) GetQuery(input string, out chan map[string]interface{}) {
	defer close(out)
	s.shape = make(map[string]interface{})
	s.wantShape = true
	s.env.Run(input)
	out <- s.shape
	s.shape = nil
}

func (s *Session) InputParses(input string) (query.ParseResult, error) {
	script, err := s.env.Compile("", input)
	if err != nil {
		return query.ParseFail, err
	}
	s.script = script
	return query.Parsed, nil
}

func (s *Session) SendResult(result *GremlinResult) bool {
	if s.limit >= 0 && s.limit == s.count {
		return false
	}
	select {
	case <-s.kill:
		return false
	default:
	}
	if s.results != nil {
		s.results <- result
		s.count++
		if s.limit >= 0 && s.limit == s.count {
			return false
		} else {
			return true
		}
	}
	return false
}

func (s *Session) runUnsafe(input interface{}) (otto.Value, error) {
	s.kill = make(chan struct{})
	defer func() {
		if r := recover(); r != nil {
			if r == ErrKillTimeout {
				s.err = ErrKillTimeout
				return
			}
			panic(r)
		}
	}()

	// Use buffered chan to prevent blocking.
	s.env.Interrupt = make(chan func(), 1)

	if s.timeoutSec >= 0 {
		go func() {
			time.Sleep(s.timeoutSec * time.Second)
			close(s.kill)
			s.envLock.Lock()
			defer s.envLock.Unlock()
			if s.env != nil {
				s.env.Interrupt <- func() {
					panic(ErrKillTimeout)
				}
				s.env = s.emptyEnv
			}
		}()
	}

	s.envLock.Lock()
	defer s.envLock.Unlock()
	return s.env.Run(input)
}

func (s *Session) ExecInput(input string, out chan interface{}, limit int) {
	defer close(out)
	s.err = nil
	s.results = out
	var err error
	var value otto.Value
	if s.script == nil {
		value, err = s.runUnsafe(input)
	} else {
		value, err = s.runUnsafe(s.script)
	}
	out <- &GremlinResult{
		metaresult: true,
		err:        err,
		val:        &value,
	}
	s.results = nil
	s.script = nil
	s.envLock.Lock()
	s.env = s.emptyEnv
	s.envLock.Unlock()
}

func (s *Session) ToText(result interface{}) string {
	data := result.(*GremlinResult)
	if data.metaresult {
		if data.err != nil {
			return fmt.Sprintf("Error: %v\n", data.err)
		}
		if data.val != nil {
			s, _ := data.val.Export()
			if data.val.IsObject() {
				typeVal, _ := data.val.Object().Get("_gremlin_type")
				if !typeVal.IsUndefined() {
					s = "[internal Iterator]"
				}
			}
			return fmt.Sprintln("=>", s)
		}
		return ""
	}
	var out string
	out = fmt.Sprintln("****")
	if data.val == nil {
		tags := data.actualResults
		tagKeys := make([]string, len(*tags))
		i := 0
		for k, _ := range *tags {
			tagKeys[i] = k
			i++
		}
		sort.Strings(tagKeys)
		for _, k := range tagKeys {
			if k == "$_" {
				continue
			}
			out += fmt.Sprintf("%s : %s\n", k, s.ts.NameOf((*tags)[k]))
		}
	} else {
		if data.val.IsObject() {
			export, _ := data.val.Export()
			mapExport := export.(map[string]string)
			for k, v := range mapExport {
				out += fmt.Sprintf("%s : %v\n", k, v)
			}
		} else {
			strVersion, _ := data.val.ToString()
			out += fmt.Sprintf("%s\n", strVersion)
		}
	}
	return out
}

// Web stuff
func (s *Session) BuildJson(result interface{}) {
	data := result.(*GremlinResult)
	if !data.metaresult {
		if data.val == nil {
			obj := make(map[string]string)
			tags := data.actualResults
			tagKeys := make([]string, len(*tags))
			i := 0
			for k, _ := range *tags {
				tagKeys[i] = k
				i++
			}
			sort.Strings(tagKeys)
			for _, k := range tagKeys {
				obj[k] = s.ts.NameOf((*tags)[k])
			}
			s.dataOutput = append(s.dataOutput, obj)
		} else {
			if data.val.IsObject() {
				export, _ := data.val.Export()
				s.dataOutput = append(s.dataOutput, export)
			} else {
				strVersion, _ := data.val.ToString()
				s.dataOutput = append(s.dataOutput, strVersion)
			}
		}
	}
}

func (s *Session) GetJson() ([]interface{}, error) {
	defer s.ClearJson()
	if s.err != nil {
		return nil, s.err
	}
	select {
	case <-s.kill:
		return nil, ErrKillTimeout
	default:
		return s.dataOutput, nil
	}
}

func (s *Session) ClearJson() {
	s.dataOutput = nil
}
