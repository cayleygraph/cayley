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

// +build appengine appenginevm

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/cayleygraph/cayley/clog"

	"github.com/cayleygraph/cayley/internal/config"
	"github.com/cayleygraph/cayley/internal/db"
	"github.com/cayleygraph/cayley/internal/http"

	_ "github.com/cayleygraph/cayley/graph/gaedatastore"
	_ "github.com/cayleygraph/cayley/writer"

	// Register supported query languages
	_ "github.com/cayleygraph/cayley/query/graphql"
	_ "github.com/cayleygraph/cayley/query/gremlin"
	_ "github.com/cayleygraph/cayley/query/mql"
)

var (
	quadFile           = ""
	quadType           = "cquad"
	cpuprofile         = ""
	queryLanguage      = "gremlin"
	configFile         = ""
	databasePath       = ""
	databaseBackend    = "gaedatastore"
	replicationBackend = "single"
	host               = "127.0.0.1"
	loadSize           = 100
	port               = "64210"
	readOnly           = false
	timeout            = 30 * time.Second
)

func configFrom(file string) (*config.Config, error) {
	// Find the file...
	if file != "" {
		if _, err := os.Stat(file); os.IsNotExist(err) {
			return nil, fmt.Errorf("Cannot find specified configuration file", file)
		}
	} else if _, err := os.Stat("/cayley_appengine.cfg"); err == nil {
		file = "/cayley_appengine.cfg"
	}
	if file == "" {
		clog.Infof("Couldn't find a config file appengine.cfg. Going by flag defaults only.")
	}
	cfg, err := config.Load(file)
	if err != nil {
		return nil, err
	}

	if cfg.DatabasePath == "" {
		cfg.DatabasePath = databasePath
	}

	if cfg.DatabaseType == "" {
		cfg.DatabaseType = databaseBackend
	}

	if cfg.ReplicationType == "" {
		cfg.ReplicationType = replicationBackend
	}

	if cfg.ListenHost == "" {
		cfg.ListenHost = host
	}

	if cfg.ListenPort == "" {
		cfg.ListenPort = port
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = timeout
	}

	if cfg.LoadSize == 0 {
		cfg.LoadSize = loadSize
	}

	cfg.ReadOnly = cfg.ReadOnly || readOnly

	return cfg, nil
}

func init() {
	cfg, err := configFrom("cayley_appengine.cfg")
	if err != nil {
		clog.Fatalf("Error loading config: %v", err)
	}

	handle, err := db.Open(cfg)
	if err != nil {
		clog.Fatalf("Error opening database: %v", err)
	}
	http.SetupRoutes(handle, cfg)
}
