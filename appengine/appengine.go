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

// +build appengine

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/barakmich/glog"

	"github.com/google/cayley/internal/config"
	"github.com/google/cayley/internal/db"
	"github.com/google/cayley/internal/http"

	_ "github.com/google/cayley/graph/gaedatastore"
	_ "github.com/google/cayley/writer"
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
		glog.Infoln("Couldn't find a config file appengine.cfg. Going by flag defaults only.")
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
	glog.SetToStderr(true)
	cfg, err := configFrom("cayley_appengine.cfg")
	if err != nil {
		glog.Fatalln("Error loading config:", err)
	}

	handle, err := db.Open(cfg)
	if err != nil {
		glog.Fatalln("Error opening database:", err)
	}
	http.SetupRoutes(handle, cfg)
}
