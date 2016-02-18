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
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/peterh/liner"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/internal/config"
	"github.com/google/cayley/quad/cquads"
	"github.com/google/cayley/query"
	"github.com/google/cayley/query/gremlin"
	"github.com/google/cayley/query/mql"
	"github.com/google/cayley/query/sexp"
)

func trace(s string) (string, time.Time) {
	return s, time.Now()
}

func un(s string, startTime time.Time) {
	endTime := time.Now()

	fmt.Printf(s, float64(endTime.UnixNano()-startTime.UnixNano())/float64(1E6))
}

func Run(query string, ses query.Session) {
	nResults := 0
	startTrace, startTime := trace("Elapsed time: %g ms\n\n")
	defer func() {
		if nResults > 0 {
			un(startTrace, startTime)
		}
	}()
	fmt.Printf("\n")
	c := make(chan interface{}, 5)
	go ses.Execute(query, c, 100)
	for res := range c {
		fmt.Print(ses.Format(res))
		nResults++
	}
	if nResults > 0 {
		results := "Result"
		if nResults > 1 {
			results += "s"
		}
		fmt.Printf("-----------\n%d %s\n", nResults, results)
	}
}

const (
	ps1 = "cayley> "
	ps2 = "...     "

	history = ".cayley_history"
)

func Repl(h *graph.Handle, queryLanguage string, cfg *config.Config) error {
	var ses query.Session
	switch queryLanguage {
	case "sexp":
		ses = sexp.NewSession(h.QuadStore)
	case "mql":
		ses = mql.NewSession(h.QuadStore)
	case "gremlin":
		fallthrough
	default:
		ses = gremlin.NewSession(h.QuadStore, cfg.Timeout, true)
	}

	term, err := terminal(history)
	if os.IsNotExist(err) {
		fmt.Printf("creating new history file: %q\n", history)
	}
	defer persist(term, history)

	var (
		prompt = ps1

		code string
	)

	for {
		if len(code) == 0 {
			prompt = ps1
		} else {
			prompt = ps2
		}
		line, err := term.Prompt(prompt)
		if err != nil {
			if err == io.EOF {
				fmt.Println()
				return nil
			}
			return err
		}

		term.AppendHistory(line)

		line = strings.TrimSpace(line)
		if len(line) == 0 || line[0] == '#' {
			continue
		}

		if code == "" {
			cmd, args := splitLine(line)

			switch cmd {
			case ":debug":
				args = strings.TrimSpace(args)
				var debug bool
				switch args {
				case "t":
					debug = true
				case "f":
					// Do nothing.
				default:
					debug, err = strconv.ParseBool(args)
					if err != nil {
						fmt.Printf("Error: cannot parse %q as a valid boolean - acceptable values: 't'|'true' or 'f'|'false'\n", args)
						continue
					}
				}
				ses.Debug(debug)
				fmt.Printf("Debug set to %t\n", debug)
				continue

			case ":a":
				quad, err := cquads.Parse(args)
				if err != nil {
					fmt.Printf("Error: not a valid quad: %v\n", err)
					continue
				}

				h.QuadWriter.AddQuad(quad)
				continue

			case ":d":
				quad, err := cquads.Parse(args)
				if err != nil {
					fmt.Printf("Error: not a valid quad: %v\n", err)
					continue
				}
				err = h.QuadWriter.RemoveQuad(quad)
				if err != nil {
					fmt.Printf("error deleting: %v\n", err)
				}
				continue

			case "exit":
				term.Close()
				os.Exit(0)

			default:
				if cmd[0] == ':' {
					fmt.Printf("Unknown command: %q\n", cmd)
					continue
				}
			}
		}

		code += line

		result, err := ses.Parse(code)
		switch result {
		case query.Parsed:
			Run(code, ses)
			code = ""
		case query.ParseFail:
			fmt.Println("Error: ", err)
			code = ""
		case query.ParseMore:
		}
	}
}

// Splits a line into a command and its arguments
// e.g. ":a b c d ." will be split into ":a" and " b c d ."
func splitLine(line string) (string, string) {
	var command, arguments string

	line = strings.TrimSpace(line)

	// An empty line/a line consisting of whitespace contains neither command nor arguments
	if len(line) > 0 {
		command = strings.Fields(line)[0]

		// A line containing only a command has no arguments
		if len(line) > len(command) {
			arguments = line[len(command):]
		}
	}

	return command, arguments
}

func terminal(path string) (*liner.State, error) {
	term := liner.NewLiner()

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, os.Kill)
		<-c

		err := persist(term, history)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to properly clean up terminal: %v\n", err)
			os.Exit(1)
		}

		os.Exit(0)
	}()

	f, err := os.Open(path)
	if err != nil {
		return term, err
	}
	defer f.Close()
	_, err = term.ReadHistory(f)
	return term, err
}

func persist(term *liner.State, path string) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("could not open %q to append history: %v", path, err)
	}
	defer f.Close()
	_, err = term.WriteHistory(f)
	if err != nil {
		return fmt.Errorf("could not write history to %q: %v", path, err)
	}
	return term.Close()
}
