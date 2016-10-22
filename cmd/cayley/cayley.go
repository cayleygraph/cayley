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
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"golang.org/x/net/context"

	"github.com/cayleygraph/cayley/clog"
	_ "github.com/cayleygraph/cayley/clog/glog"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/internal"
	"github.com/cayleygraph/cayley/internal/config"
	"github.com/cayleygraph/cayley/internal/db"
	"github.com/cayleygraph/cayley/internal/http"

	// Load all supported backends.
	_ "github.com/cayleygraph/cayley/graph/bolt"
	_ "github.com/cayleygraph/cayley/graph/leveldb"
	_ "github.com/cayleygraph/cayley/graph/memstore"
	_ "github.com/cayleygraph/cayley/graph/mongo"
	_ "github.com/cayleygraph/cayley/graph/sql"

	// Load all supported quad formats.
	_ "github.com/cayleygraph/cayley/quad/gml"
	_ "github.com/cayleygraph/cayley/quad/graphml"
	_ "github.com/cayleygraph/cayley/quad/json"
	_ "github.com/cayleygraph/cayley/quad/jsonld"
	_ "github.com/cayleygraph/cayley/quad/nquads"
	_ "github.com/cayleygraph/cayley/quad/pquads"

	// Load writer registry
	_ "github.com/cayleygraph/cayley/writer"

	// Load supported query languages
	_ "github.com/cayleygraph/cayley/query/gremlin"
	_ "github.com/cayleygraph/cayley/query/mql"
	_ "github.com/cayleygraph/cayley/query/sexp"
)

var (
	quadFile           = flag.String("quads", "", "Quad file to load before going to REPL.")
	initOpt            = flag.Bool("init", false, "Initialize the database before using it. Equivalent to running `cayley init` followed by the given command.")
	quadType           = flag.String("format", "cquad", `Quad format to use for loading ("cquad" or "nquad").`)
	cpuprofile         = flag.String("prof", "", "Output profiling file.")
	queryLanguage      = flag.String("query_lang", "gremlin", "Use this parser as the query language.")
	configFile         = flag.String("config", "", "Path to an explicit configuration file.")
	databasePath       = flag.String("dbpath", "/tmp/testdb", "Path to the database.")
	databaseBackend    = flag.String("db", "memstore", "Database Backend.")
	dumpFile           = flag.String("dump", "dbdump.nq", `Quad file to dump the database to (".gz" supported, "-" for stdout).`)
	dumpType           = flag.String("dump_type", "quad", `Quad file format ("json", "quad", "gml", "graphml").`)
	replicationBackend = flag.String("replication", "single", "Replication method.")
	host               = flag.String("host", "127.0.0.1", "Host to listen on (defaults to all).")
	loadSize           = flag.Int("load_size", 10000, "Size of quadsets to load")
	port               = flag.String("port", "64210", "Port to listen on.")
	readOnly           = flag.Bool("read_only", false, "Disable writing via HTTP.")
	timeout            = flag.Duration("timeout", 30*time.Second, "Elapsed time until an individual query times out.")
)

// Filled in by `go build ldflags="-X main.Version `ver`"`.
var (
	BuildDate string
	Version   string
)

func usage() {
	fmt.Fprintln(os.Stderr, `
Usage:
  cayley COMMAND [flags]

Commands:
  init      Create an empty database.
  load      Bulk-load a quad file into the database.
  http      Serve an HTTP endpoint on the given host and port.
  dump      Bulk-dump the database into a quad file.
  repl      Drop into a REPL of the given query language.
  version   Version information.

Flags:`)
	flag.PrintDefaults()
}

func init() {
	flag.Usage = usage

	flag.BoolVar(&graph.IgnoreDuplicates, "ignoredup", false, "Don't stop loading on duplicated key on add")
	flag.BoolVar(&graph.IgnoreMissing, "ignoremissing", false, "Don't stop loading on missing key on delete")
}

func configFrom(file string) *config.Config {
	// Find the file...
	if file != "" {
		if _, err := os.Stat(file); os.IsNotExist(err) {
			clog.Fatalf("Cannot find specified configuration file '%s', aborting.", file)
		}
	} else if _, err := os.Stat(os.Getenv("CAYLEY_CFG")); err == nil {
		file = os.Getenv("CAYLEY_CFG")
	} else if _, err := os.Stat("/etc/cayley.cfg"); err == nil {
		file = "/etc/cayley.cfg"
	}
	if file == "" {
		clog.Infof("Couldn't find a config file in either $CAYLEY_CFG or /etc/cayley.cfg. Going by flag defaults only.")
	}
	cfg, err := config.Load(file)
	if err != nil {
		clog.Fatalf("%v", err)
	}

	if cfg.DatabasePath == "" {
		cfg.DatabasePath = *databasePath
	}

	if cfg.DatabaseType == "" {
		cfg.DatabaseType = *databaseBackend
	}

	if cfg.ReplicationType == "" {
		cfg.ReplicationType = *replicationBackend
	}

	if cfg.ListenHost == "" {
		cfg.ListenHost = *host
	}

	if cfg.ListenPort == "" {
		cfg.ListenPort = *port
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = *timeout
	}

	if cfg.LoadSize == 0 {
		cfg.LoadSize = *loadSize
	}

	cfg.ReadOnly = cfg.ReadOnly || *readOnly

	return cfg
}

func main() {
	// No command? It's time for usage.
	if len(os.Args) == 1 {
		fmt.Fprintln(os.Stderr, "Cayley is a graph store and graph query layer.")
		usage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	os.Args = append(os.Args[:1], os.Args[2:]...)
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			clog.Fatalf("%v", err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	var buildString string
	if Version != "" {
		buildString = fmt.Sprint("Cayley ", Version, " built ", BuildDate)
		clog.Infof(buildString)
	}

	cfg := configFrom(*configFile)

	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
		clog.Infof("Setting GOMAXPROCS to %d", runtime.NumCPU())
	} else {
		clog.Infof("GOMAXPROCS currently %v -- not adjusting", os.Getenv("GOMAXPROCS"))
	}

	var (
		handle *graph.Handle
		err    error
	)
	switch cmd {
	case "version":
		if Version != "" {
			fmt.Println(buildString)
		} else {
			fmt.Println("Cayley snapshot")
		}
		os.Exit(0)

	case "init":
		err = db.Init(cfg)
		if err != nil {
			break
		}
		if *quadFile != "" {
			handle, err = db.Open(cfg)
			if err != nil {
				break
			}
			err = internal.Load(handle.QuadWriter, cfg.LoadSize, *quadFile, *quadType)
			if err != nil {
				break
			}
			handle.Close()
		}

	case "load":
		handle, err = db.Open(cfg)
		if err != nil {
			break
		}
		err = internal.Load(handle.QuadWriter, cfg.LoadSize, *quadFile, *quadType)
		if err != nil {
			break
		}

		handle.Close()

	case "dump":
		handle, err = db.Open(cfg)
		if err != nil {
			break
		}
		if !graph.IsPersistent(cfg.DatabaseType) {
			err = internal.Load(handle.QuadWriter, cfg.LoadSize, *quadFile, *quadType)
			if err != nil {
				break
			}
		}

		err = internal.Dump(handle.QuadStore, *dumpFile, *dumpType)
		if err != nil {
			break
		}

		handle.Close()

	case "repl":
		if *initOpt {
			err = db.Init(cfg)
			if err != nil && err != graph.ErrDatabaseExists {
				break
			}
		}
		handle, err = db.Open(cfg)
		if err != nil {
			break
		}
		if !graph.IsPersistent(cfg.DatabaseType) {
			err = internal.Load(handle.QuadWriter, cfg.LoadSize, "", *quadType)
			if err != nil {
				break
			}
		}

		err = db.Repl(context.TODO(), handle, *queryLanguage, cfg.Timeout)

		handle.Close()

	case "http":
		if *initOpt {
			err = db.Init(cfg)
			if err != nil && err != graph.ErrDatabaseExists {
				break
			}
		}
		handle, err = db.Open(cfg)
		if err != nil {
			break
		}
		if !graph.IsPersistent(cfg.DatabaseType) {
			err = internal.Load(handle.QuadWriter, cfg.LoadSize, "", *quadType)
			if err != nil {
				break
			}
		}

		http.Serve(handle, cfg)

		handle.Close()

	default:
		fmt.Println("No command", cmd)
		usage()
	}
	if err != nil {
		clog.Errorf("%v", err)
	}
}
