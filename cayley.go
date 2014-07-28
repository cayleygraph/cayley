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

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/barakmich/glog"

	"github.com/google/cayley/config"
	"github.com/google/cayley/db"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/http"

	// Load all supported backends.
	_ "github.com/google/cayley/graph/leveldb"
	_ "github.com/google/cayley/graph/memstore"
	_ "github.com/google/cayley/graph/mongo"
)

var tripleFile = flag.String("triples", "", "Triple File to load before going to REPL.")
var cpuprofile = flag.String("prof", "", "Output profiling file.")
var queryLanguage = flag.String("query_lang", "gremlin", "Use this parser as the query language.")
var configFile = flag.String("config", "", "Path to an explicit configuration file.")
var stdin = flag.Bool("stdin", false, "Whether or not to load data from standard in")

func Usage() {
	fmt.Println("Cayley is a graph store and graph query layer.")
	fmt.Println("\nUsage:")
	fmt.Println("  cayley COMMAND [flags]")
	fmt.Println("\nCommands:")
	fmt.Println("  init\tCreate an empty database.")
	fmt.Println("  load\tBulk-load a triple file into the database.")
	fmt.Println("  http\tServe an HTTP endpoint on the given host and port.")
	fmt.Println("  repl\tDrop into a REPL of the given query language.")
	fmt.Println("\nFlags:")
	flag.Parse()
	flag.PrintDefaults()
}

func main() {
	// No command? It's time for usage.
	if len(os.Args) == 1 {
		Usage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	newargs := make([]string, 0)
	newargs = append(newargs, os.Args[0])
	newargs = append(newargs, os.Args[2:]...)
	os.Args = newargs
	flag.Parse()

	cfg := config.ParseConfigFromFlagsAndFile(*configFile)

	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
		glog.Infoln("Setting GOMAXPROCS to", runtime.NumCPU())
	} else {
		glog.Infoln("GOMAXPROCS currently", os.Getenv("GOMAXPROCS"), " -- not adjusting")
	}

	var (
		ts  graph.TripleStore
		err error
	)
	switch cmd {
	case "init":
		err = db.Init(cfg, *tripleFile, *stdin)
	case "load":
		ts, err = db.Open(cfg, *stdin)
		if err != nil {
			break
		}
		err = db.Load(ts, cfg, *tripleFile, *stdin)
		if err != nil {
			break
		}
		ts.Close()
	case "repl":
		if *stdin {
			err = errors.New("cannot use repl while loading data from stdin")
			break
		}
		ts, err = db.Open(cfg, *stdin)

		if err != nil {
			break
		}

		err = db.Repl(ts, *queryLanguage, cfg)
		if err != nil {
			break
		}

		ts.Close()
	case "http":
		ts, err = db.Open(cfg, *stdin)
		if err != nil {
			break
		}
		http.Serve(ts, cfg)
		ts.Close()
	default:
		fmt.Println("No command", cmd)
		flag.Usage()
	}
	if err != nil {
		glog.Fatalln(err)
	}
}
