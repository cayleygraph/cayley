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

	"github.com/barakmich/glog"

	"github.com/google/cayley/config"
	"github.com/google/cayley/db"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/http"
	"github.com/google/cayley/quad"
	"github.com/google/cayley/quad/cquads"
	"github.com/google/cayley/quad/nquads"

	// Load all supported backends.
	_ "github.com/google/cayley/graph/leveldb"
	_ "github.com/google/cayley/graph/memstore"
	_ "github.com/google/cayley/graph/mongo"

	// Load writer registry
	_ "github.com/google/cayley/writer"
)

var (
	tripleFile    = flag.String("triples", "", "Triple File to load before going to REPL.")
	tripleType    = flag.String("format", "cquad", `Triple format to use for loading ("cquad" or "nquad").`)
	cpuprofile    = flag.String("prof", "", "Output profiling file.")
	queryLanguage = flag.String("query_lang", "gremlin", "Use this parser as the query language.")
	configFile    = flag.String("config", "", "Path to an explicit configuration file.")
)

// Filled in by `go build ldflags="-X main.VERSION `ver`"`.
var (
	BUILD_DATE string
	VERSION    string
)

func Usage() {
	fmt.Println("Cayley is a graph store and graph query layer.")
	fmt.Println("\nUsage:")
	fmt.Println("  cayley COMMAND [flags]")
	fmt.Println("\nCommands:")
	fmt.Println("  init      Create an empty database.")
	fmt.Println("  load      Bulk-load a triple file into the database.")
	fmt.Println("  http      Serve an HTTP endpoint on the given host and port.")
	fmt.Println("  repl      Drop into a REPL of the given query language.")
	fmt.Println("  version   Version information.")
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
	var newargs []string
	newargs = append(newargs, os.Args[0])
	newargs = append(newargs, os.Args[2:]...)
	os.Args = newargs
	flag.Parse()

	var buildString string
	if VERSION != "" {
		buildString = fmt.Sprint("Cayley ", VERSION, " built ", BUILD_DATE)
		glog.Infoln(buildString)
	}

	cfg := config.ParseConfigFromFlagsAndFile(*configFile)

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
		if VERSION != "" {
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
		if *tripleFile != "" {
			handle, err = db.Open(cfg)
			if err != nil {
				break
			}
			err = load(handle.QuadWriter, cfg, *tripleFile, *tripleType)
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
		err = load(handle.QuadWriter, cfg, *tripleFile, *tripleType)
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
			err = load(handle.QuadWriter, cfg, "", *tripleType)
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
			err = load(handle.QuadWriter, cfg, "", *tripleType)
			if err != nil {
				break
			}
		}

		http.Serve(handle, cfg)

		handle.Close()

	default:
		fmt.Println("No command", cmd)
		flag.Usage()
	}
	if err != nil {
		glog.Errorln(err)
	}
}

func load(qw graph.QuadWriter, cfg *config.Config, path, typ string) error {
	var r io.Reader

	if path == "" {
		path = cfg.DatabasePath
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
