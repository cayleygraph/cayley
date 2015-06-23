// Copyright 2015 The Cayley Authors. All rights reserved.
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

	"github.com/google/cayley/config"
	_ "github.com/google/cayley/graph"

	// Load all supported backends.
	"github.com/google/cayley/db"
	_ "github.com/google/cayley/graph/bolt"
	_ "github.com/google/cayley/graph/leveldb"
	_ "github.com/google/cayley/graph/memstore"
	_ "github.com/google/cayley/graph/mongo"
	"github.com/google/cayley/internal"
)

var (
	configFile      = flag.String("config", "", "Path to an explicit configuration file.")
	databasePath    = flag.String("dbpath", "/tmp/testdb", "Path to the database.")
	databaseBackend = flag.String("db", "memstore", "Database Backend.")
)

func configFrom(file string) *config.Config {
	if cfg.DatabasePath == "" {
		cfg.DatabasePath = *databasePath
	}

	if cfg.DatabaseType == "" {
		cfg.DatabaseType = *databaseBackend
	}
	return cfg
}

func main() {
	flag.Parse()
	cfg := configFrom(*configFile)

	r, registered := storeRegistry[*databaseBackend]
	if registered {
		return r.initFunc(dbpath, opts)
	}
	return errors.New("quadstore: name '" + name + "' is not registered")
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
}
