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

// +build !appengine

package repl

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/peterh/liner"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/quad/nquads"
)

func trace(s string) (string, time.Time) {
	return s, time.Now()
}

func un(s string, startTime time.Time) {
	endTime := time.Now()

	fmt.Printf(s, float64(endTime.UnixNano()-startTime.UnixNano())/float64(1e6))
}

func Run(ctx context.Context, qu string, ses query.REPLSession) error {
	nResults := 0
	startTrace, startTime := trace("Elapsed time: %g ms\n\n")
	defer func() {
		if nResults > 0 {
			un(startTrace, startTime)
		}
	}()
	fmt.Printf("\n")
	it, err := ses.Execute(ctx, qu, query.Options{
		Collation: query.REPL,
		Limit:     100,
	})
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Next(ctx) {
		fmt.Print(it.Result())
		nResults++
	}
	if err := it.Err(); err != nil {
		return err
	}
	if nResults > 0 {
		results := "Result"
		if nResults > 1 {
			results += "s"
		}
		fmt.Printf("-----------\n%d %s\n", nResults, results)
	}
	return nil
}

const (
	defaultLanguage = "gizmo"

	ps1 = "cayley> "
	ps2 = "...     "

	history = ".cayley_history"
)

func Repl(ctx context.Context, h *graph.Handle, queryLanguage string, timeout time.Duration) error {
	if queryLanguage == "" {
		queryLanguage = defaultLanguage
	}
	l := query.GetLanguage(queryLanguage)
	if l == nil || l.Session == nil {
		return fmt.Errorf("unsupported query language: %q", queryLanguage)
	}
	ses := l.Session(h.QuadStore)

	term, err := terminal(history)
	if os.IsNotExist(err) {
		fmt.Printf("creating new history file: %q\n", history)
	}
	defer persist(term, history)

	var (
		prompt = ps1

		code string
	)

	newCtx := func() (context.Context, func()) { return ctx, func() {} }
	if timeout > 0 {
		newCtx = func() (context.Context, func()) { return context.WithTimeout(ctx, timeout) }
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
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
				if debug {
					clog.SetV(2)
				} else {
					clog.SetV(0)
				}
				fmt.Printf("Debug set to %t\n", debug)
				continue

			case ":a":
				quad, err := nquads.Parse(args)
				if err == nil {
					err = h.QuadWriter.AddQuad(quad)
				}
				if err != nil {
					fmt.Printf("Error: not a valid quad: %v\n", err)
					continue
				}
				continue

			case ":d":
				quad, err := nquads.Parse(args)
				if err != nil {
					fmt.Printf("Error: not a valid quad: %v\n", err)
					continue
				}
				err = h.QuadWriter.RemoveQuad(quad)
				if err != nil {
					fmt.Printf("error deleting: %v\n", err)
				}
				continue

			case "help":
				fmt.Printf("Help\n\texit // Exit\n\thelp // this help\n\td: <quad> // delete quad\n\ta: <quad> // add quad\n\t:debug [t|f]\n")
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

		nctx, cancel := newCtx()
		err = Run(nctx, code, ses)
		cancel()
		if err == query.ErrParseMore {
			// collect more input
		} else if err != nil {
			fmt.Println("Error: ", err)
			code = ""
		} else {
			code = ""
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
