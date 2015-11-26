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

	"github.com/barakmich/glog"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/internal"
	"github.com/google/cayley/internal/config"
	"github.com/google/cayley/internal/db"
	"github.com/google/cayley/internal/http"

	// Load all supported backends.
	_ "github.com/google/cayley/graph/bolt"
	_ "github.com/google/cayley/graph/leveldb"
	_ "github.com/google/cayley/graph/memstore"
	_ "github.com/google/cayley/graph/mongo"
	_ "github.com/google/cayley/graph/sql"

	// Load writer registry
	_ "github.com/google/cayley/writer"
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
}

func configFrom(file string) *config.Config {
	// Find the file...
	if file != "" {
		if _, err := os.Stat(file); os.IsNotExist(err) {
			glog.Fatalln("Cannot find specified configuration file", file, ", aborting.")
		}
	} else if _, err := os.Stat(os.Getenv("CAYLEY_CFG")); err == nil {
		file = os.Getenv("CAYLEY_CFG")
	} else if _, err := os.Stat("/etc/cayley.cfg"); err == nil {
		file = "/etc/cayley.cfg"
	}
	if file == "" {
		glog.Infoln("Couldn't find a config file in either $CAYLEY_CFG or /etc/cayley.cfg. Going by flag defaults only.")
	}
	cfg, err := config.Load(file)
	if err != nil {
		glog.Fatalln(err)
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
			glog.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	var buildString string
	if Version != "" {
		buildString = fmt.Sprint("Cayley ", Version, " built ", BuildDate)
		glog.Infoln(buildString)
	}

	cfg := configFrom(*configFile)

	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
		glog.Infoln("Setting GOMAXPROCS to", runtime.NumCPU())
	} else {
		glog.Infoln("GOMAXPROCS currently", os.Getenv("GOMAXPROCS"), " -- not adjusting")
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
			err = internal.Load(handle.QuadWriter, cfg, *quadFile, *quadType)
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
		err = internal.Load(handle.QuadWriter, cfg, *quadFile, *quadType)
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
			err = internal.Load(handle.QuadWriter, cfg, *quadFile, *quadType)
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
			err = internal.Load(handle.QuadWriter, cfg, "", *quadType)
			if err != nil {
				break
			}
		}

		err = db.Repl(handle, *queryLanguage, cfg)

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
			err = internal.Load(handle.QuadWriter, cfg, "", *quadType)
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
		glog.Errorln(err)
	}
}
