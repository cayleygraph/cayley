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

package db

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/google/cayley/config"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/sexp"
	"github.com/google/cayley/quad/cquads"
	"github.com/google/cayley/query/gremlin"
	"github.com/google/cayley/query/mql"
)

func trace(s string) (string, time.Time) {
	return s, time.Now()
}

func un(s string, startTime time.Time) {
	endTime := time.Now()

	fmt.Printf(s, float64(endTime.UnixNano()-startTime.UnixNano())/float64(1E6))
}

func Run(query string, ses graph.Session) {
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

func Repl(ts graph.TripleStore, queryLanguage string, cfg *config.Config) error {
	var ses graph.Session
	switch queryLanguage {
	case "sexp":
		ses = sexp.NewSession(ts)
	case "mql":
		ses = mql.NewSession(ts)
	case "gremlin":
		fallthrough
	default:
		ses = gremlin.NewSession(ts, cfg.GremlinTimeout, true)
	}
	buf := bufio.NewReader(os.Stdin)
	var line []byte
	for {
		if len(line) == 0 {
			fmt.Print("cayley> ")
		} else {
			fmt.Print("...       ")
		}
		l, prefix, err := buf.ReadLine()
		if err == io.EOF {
			if len(line) != 0 {
				line = line[:0]
			} else {
				return nil
			}
		}
		if err != nil {
			line = line[:0]
		}
		if prefix {
			return errors.New("line too long")
		}
		line = append(line, l...)
		if len(line) == 0 {
			continue
		}
		line = bytes.TrimSpace(line)
		if len(line) == 0 || line[0] == '#' {
			line = line[:0]
			continue
		}
		if bytes.HasPrefix(line, []byte(":debug")) {
			ses.ToggleDebug()
			fmt.Println("Debug Toggled")
			line = line[:0]
			continue
		}
		if bytes.HasPrefix(line, []byte(":a")) {
			var tripleStmt = line[3:]
			triple, err := cquads.Parse(string(tripleStmt))
			if triple == nil {
				if err != nil {
					fmt.Printf("not a valid triple: %v\n", err)
				}
				line = line[:0]
				continue
			}
			ts.AddTriple(triple)
			line = line[:0]
			continue
		}
		if bytes.HasPrefix(line, []byte(":d")) {
			var tripleStmt = line[3:]
			triple, err := cquads.Parse(string(tripleStmt))
			if triple == nil {
				if err != nil {
					fmt.Printf("not a valid triple: %v\n", err)
				}
				line = line[:0]
				continue
			}
			ts.RemoveTriple(triple)
			line = line[:0]
			continue
		}
		result, err := ses.InputParses(string(line))
		switch result {
		case graph.Parsed:
			Run(string(line), ses)
			line = line[:0]
		case graph.ParseFail:
			fmt.Println("Error: ", err)
			line = line[:0]
		case graph.ParseMore:
		}
	}
}
