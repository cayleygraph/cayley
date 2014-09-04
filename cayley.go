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
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	client "net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/barakmich/glog"

	"github.com/google/cayley/config"
	"github.com/google/cayley/db"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/http"
	"github.com/google/cayley/quad"
	"github.com/google/cayley/quad/cquads"
	"github.com/google/cayley/quad/nquads"

	// Load all supported backends.
	_ "github.com/google/cayley/graph/bolt"
	_ "github.com/google/cayley/graph/leveldb"
	_ "github.com/google/cayley/graph/memstore"
	_ "github.com/google/cayley/graph/mongo"

	// Load writer registry
	_ "github.com/google/cayley/writer"
)

var (
	quadFile           = flag.String("quads", "", "Quad file to load before going to REPL.")
	quadType           = flag.String("format", "cquad", `Quad format to use for loading ("cquad" or "nquad").`)
	cpuprofile         = flag.String("prof", "", "Output profiling file.")
	queryLanguage      = flag.String("query_lang", "gremlin", "Use this parser as the query language.")
	configFile         = flag.String("config", "", "Path to an explicit configuration file.")
	databasePath       = flag.String("dbpath", "/tmp/testdb", "Path to the database.")
	databaseBackend    = flag.String("db", "memstore", "Database Backend.")
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
			err = load(handle.QuadWriter, cfg, *quadFile, *quadType)
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
		err = load(handle.QuadWriter, cfg, *quadFile, *quadType)
		if err != nil {
			break
		}

		handle.Close()

	case "repl":
		handle, err = db.Open(cfg)
		if err != nil {
			break
		}
		if !graph.IsPersistent(cfg.DatabaseType) {
			err = load(handle.QuadWriter, cfg, "", *quadType)
			if err != nil {
				break
			}
		}

		err = db.Repl(handle, *queryLanguage, cfg)

		handle.Close()

	case "http":
		handle, err = db.Open(cfg)
		if err != nil {
			break
		}
		if !graph.IsPersistent(cfg.DatabaseType) {
			err = load(handle.QuadWriter, cfg, "", *quadType)
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

func load(qw graph.QuadWriter, cfg *config.Config, path, typ string) error {
	return decompressAndLoad(qw, cfg, path, typ, db.Load)
}

func decompressAndLoad(qw graph.QuadWriter, cfg *config.Config, path, typ string, loadFn func(graph.QuadWriter, *config.Config, quad.Unmarshaler) error) error {
	var r io.Reader

	if path == "" {
		path = cfg.DatabasePath
	}
	if path == "" {
		return nil
	}
	u, err := url.Parse(path)
	if err != nil || u.Scheme == "file" || u.Scheme == "" {
		// Don't alter relative URL path or non-URL path parameter.
		if u.Scheme != "" && err == nil {
			// Recovery heuristic for mistyping "file://path/to/file".
			path = filepath.Join(u.Host, u.Path)
		}
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("could not open file %q: %v", path, err)
		}
		defer f.Close()
		r = f
	} else {
		res, err := client.Get(path)
		if err != nil {
			return fmt.Errorf("could not get resource <%s>: %v", u, err)
		}
		defer res.Body.Close()
		r = res.Body
	}

	r, err = decompressor(r)
	if err != nil {
		return err
	}

	var dec quad.Unmarshaler
	switch typ {
	case "cquad":
		dec = cquads.NewDecoder(r)
	case "nquad":
		dec = nquads.NewDecoder(r)
	default:
		return fmt.Errorf("unknown quad format %q", typ)
	}

	return db.Load(qw, cfg, dec)
}

const (
	gzipMagic  = "\x1f\x8b"
	b2zipMagic = "BZh"
)

func decompressor(r io.Reader) (io.Reader, error) {
	br := bufio.NewReader(r)
	buf, err := br.Peek(3)
	if err != nil {
		return nil, err
	}
	switch {
	case bytes.Compare(buf[:2], []byte(gzipMagic)) == 0:
		return gzip.NewReader(br)
	case bytes.Compare(buf[:3], []byte(b2zipMagic)) == 0:
		return bzip2.NewReader(br), nil
	default:
		return br, nil
	}
}
