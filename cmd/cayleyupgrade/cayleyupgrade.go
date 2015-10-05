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
	"flag"
	"fmt"
	"os"

	"github.com/barakmich/glog"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/internal/config"

	// Load all supported backends.

	_ "github.com/google/cayley/graph/bolt"
	_ "github.com/google/cayley/graph/leveldb"
	_ "github.com/google/cayley/graph/memstore"
	_ "github.com/google/cayley/graph/mongo"
)

var (
	configFile      = flag.String("config", "", "Path to an explicit configuration file.")
	databasePath    = flag.String("dbpath", "/tmp/testdb", "Path to the database.")
	databaseBackend = flag.String("db", "memstore", "Database Backend.")
)

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
	return cfg
}

func main() {
	flag.Parse()
	cfg := configFrom(*configFile)

	err := graph.UpgradeQuadStore(cfg.DatabaseType, cfg.DatabasePath, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
