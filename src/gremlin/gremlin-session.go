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
	"github.com/robertkrimen/otto"
	"graph"
	"sort"
	"time"
)

type GremlinSession struct {
	ts                   graph.TripleStore
	currentChannel       chan interface{}
	env                  *otto.Otto
	debug                bool
	limit                int
	count                int
	dataOutput           []interface{}
	lookingForQueryShape bool
	queryShape           map[string]interface{}
	err                  error
	script               *otto.Script
	doHalt               bool
	timeoutSec           time.Duration
}

func NewGremlinSession(inputTripleStore graph.TripleStore, timeoutSec int) *GremlinSession {
	var g GremlinSession
	g.ts = inputTripleStore
	g.env = BuildGremlinEnv(&g)
	g.limit = -1
	g.count = 0
	g.lookingForQueryShape = false
	if timeoutSec < 0 {
		g.timeoutSec = time.Duration(-1)
	} else {
		g.timeoutSec = time.Duration(timeoutSec)
	}
	g.ClearJson()
	return &g
}

type GremlinResult struct {
	metaresult    bool
	err           string
	val           *otto.Value
	actualResults *map[string]graph.TSVal
}

func (g *GremlinSession) ToggleDebug() {
	g.debug = !g.debug
}

func (g *GremlinSession) GetQuery(input string, output_struct chan map[string]interface{}) {
	defer close(output_struct)
	g.queryShape = make(map[string]interface{})
	g.lookingForQueryShape = true
	g.env.Run(input)
	output_struct <- g.queryShape
	g.queryShape = nil
}

func (g *GremlinSession) InputParses(input string) (graph.ParseResult, error) {
	script, err := g.env.Compile("", input)
	if err != nil {
		return graph.ParseFail, err
	}
	g.script = script
	return graph.Parsed, nil
}

func (g *GremlinSession) SendResult(result *GremlinResult) bool {
	if g.limit >= 0 && g.limit == g.count {
		return false
	}
	if g.doHalt {
		close(g.currentChannel)
		return false
	}
	if g.currentChannel != nil {
		g.currentChannel <- result
		g.count++
		if g.limit >= 0 && g.limit == g.count {
			return false
		} else {
			return true
		}
	}
	return false
}

var halt = errors.New("Query Timeout")

func (g *GremlinSession) runUnsafe(input interface{}) (otto.Value, error) {
	g.doHalt = false
	defer func() {
		if caught := recover(); caught != nil {
			if caught == halt {
				g.err = halt
				return
			}
			panic(caught) // Something else happened, repanic!
		}
	}()

	g.env.Interrupt = make(chan func(), 1) // The buffer prevents blocking

	if g.timeoutSec != -1 {
		go func() {
			time.Sleep(g.timeoutSec * time.Second) // Stop after two seconds
			g.doHalt = true
			if g.env != nil {
				g.env.Interrupt <- func() {
					panic(halt)
				}
				g.env = nil
			}
		}()
	}

	return g.env.Run(input) // Here be dragons (risky code)
}

func (g *GremlinSession) ExecInput(input string, out chan interface{}, limit int) {
	defer close(out)
	g.err = nil
	g.currentChannel = out
	var err error
	var value otto.Value
	if g.script == nil {
		value, err = g.runUnsafe(input)
	} else {
		value, err = g.runUnsafe(g.script)
	}
	if err != nil {
		out <- &GremlinResult{metaresult: true,
			err:           err.Error(),
			val:           &value,
			actualResults: nil}
	} else {
		out <- &GremlinResult{metaresult: true,
			err:           "",
			val:           &value,
			actualResults: nil}
	}
	g.currentChannel = nil
	g.script = nil
	g.env = nil
	return
}

func (s *GremlinSession) ToText(result interface{}) string {
	data := result.(*GremlinResult)
	if data.metaresult {
		if data.err != "" {
			return fmt.Sprintln("Error: ", data.err)
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
			out += fmt.Sprintf("%s : %s\n", k, s.ts.GetNameFor((*tags)[k]))
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
func (ses *GremlinSession) BuildJson(result interface{}) {
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
				obj[k] = ses.ts.GetNameFor((*tags)[k])
			}
			ses.dataOutput = append(ses.dataOutput, obj)
		} else {
			if data.val.IsObject() {
				export, _ := data.val.Export()
				ses.dataOutput = append(ses.dataOutput, export)
			} else {
				strVersion, _ := data.val.ToString()
				ses.dataOutput = append(ses.dataOutput, strVersion)
			}
		}
	}

}

func (ses *GremlinSession) GetJson() (interface{}, error) {
	defer ses.ClearJson()
	if ses.err != nil {
		return nil, ses.err
	}
	if ses.doHalt {
		return nil, halt
	}
	return ses.dataOutput, nil
}

func (ses *GremlinSession) ClearJson() {
	ses.dataOutput = nil
}
