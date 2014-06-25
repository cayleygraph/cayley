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

package cayley

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/google/cayley/config"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/sexp"
	"github.com/google/cayley/gremlin"
	"github.com/google/cayley/mql"
	"github.com/google/cayley/nquads"
)

func trace(s string) (string, time.Time) {
	return s, time.Now()
}

func un(s string, startTime time.Time) {
	endTime := time.Now()

	fmt.Printf(s, float64(endTime.UnixNano()-startTime.UnixNano())/float64(1E6))
}

func RunQuery(query string, ses graph.Session) {
	nResults := 0
	startTrace, startTime := trace("Elapsed time: %g ms\n\n")
	defer func() {
		if nResults > 0 {
			un(startTrace, startTime)
		}
	}()
	fmt.Printf("\n")
	c := make(chan interface{}, 5)
	go ses.ExecInput(query, c, 100)
	for res := range c {
		fmt.Print(ses.ToText(res))
		nResults++
	}
	if nResults > 0 {
		fmt.Printf("-----------\n%d Results\n", nResults)
	}
}

func CayleyRepl(ts graph.TripleStore, queryLanguage string, cfg *config.CayleyConfig) {
	var ses graph.Session
	switch queryLanguage {
	case "sexp":
		ses = sexp.NewSexpSession(ts)
	case "mql":
		ses = mql.NewMqlSession(ts)
	case "gremlin":
		fallthrough
	default:
		ses = gremlin.NewGremlinSession(ts, cfg.GremlinTimeout, true)
	}
	inputBf := bufio.NewReader(os.Stdin)
	line := ""
	for {
		if line == "" {
			fmt.Print("cayley> ")
		} else {
			fmt.Print("...       ")
		}
		l, pre, err := inputBf.ReadLine()
		if err == io.EOF {
			if line != "" {
				line = ""
			} else {
				break
			}
		}
		if err != nil {
			line = ""
		}
		if pre {
			panic("Line too long")
		}
		line += string(l)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, ":debug") {
			ses.ToggleDebug()
			fmt.Println("Debug Toggled")
			line = ""
			continue
		}
		if strings.HasPrefix(line, ":a") {
			var tripleStmt = line[3:]
			triple := nquads.ParseLineToTriple(tripleStmt)
			if triple == nil {
				fmt.Println("Not a valid triple.")
				line = ""
				continue
			}
			ts.AddTriple(triple)
			line = ""
			continue
		}
		if strings.HasPrefix(line, ":d") {
			var tripleStmt = line[3:]
			triple := nquads.ParseLineToTriple(tripleStmt)
			if triple == nil {
				fmt.Println("Not a valid triple.")
				line = ""
				continue
			}
			ts.RemoveTriple(triple)
			line = ""
			continue
		}
		result, err := ses.InputParses(line)
		switch result {
		case graph.Parsed:
			RunQuery(line, ses)
			line = ""
		case graph.ParseFail:
			fmt.Println("Error: ", err)
			line = ""
		case graph.ParseMore:
		default:
		}
	}
}
