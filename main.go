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

package main

import (
	"flag"
	"fmt"
	"github.com/barakmich/glog"
	"github.com/google/cayley/src/cayley"
	cfg "github.com/google/cayley/src/cayley/config"
	cayley_http "github.com/google/cayley/src/cayley/http"
	"github.com/google/cayley/src/graph"
	"os"
	"runtime"
)

var tripleFile = flag.String("triples", "", "Triple File to load before going to REPL.")
var cpuprofile = flag.String("prof", "", "Output profiling file.")
var queryLanguage = flag.String("query_lang", "gremlin", "Use this parser as the query language.")
var configFile = flag.String("config", "", "Path to an explicit configuration file.")

func Usage() {
	fmt.Println("Cayley is a graph store and graph query layer.\n")
	fmt.Println("Usage:")
	fmt.Println("  cayley COMMAND [flags]\n")
	fmt.Println("Commands:")
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
	var ts graph.TripleStore
	config := cfg.ParseConfigFromFlagsAndFile(*configFile)
	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
		glog.Infoln("Setting GOMAXPROCS to", runtime.NumCPU())
	} else {
		glog.Infoln("GOMAXPROCS currently", os.Getenv("GOMAXPROCS"), " -- not adjusting")
	}
	switch cmd {
	case "init":
		cayley.CayleyInit(config, *tripleFile)
	case "load":
		ts = cayley.OpenTSFromConfig(config)
		cayley.CayleyLoad(ts, config, *tripleFile, false)
		ts.Close()
	case "repl":
		ts = cayley.OpenTSFromConfig(config)
		cayley.CayleyRepl(ts, *queryLanguage, config)
		ts.Close()
	case "http":
		ts = cayley.OpenTSFromConfig(config)
		cayley_http.CayleyHTTP(ts, config)
		ts.Close()
	default:
		fmt.Println("No command", cmd)
		flag.Usage()
	}
}
