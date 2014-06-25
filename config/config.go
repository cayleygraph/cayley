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

package config

import (
	"encoding/json"
	"flag"
	"os"

	"github.com/barakmich/glog"
)

type CayleyConfig struct {
	DatabaseType    string                 `json:"database"`
	DatabasePath    string                 `json:"db_path"`
	DatabaseOptions map[string]interface{} `json:"db_options"`
	ListenHost      string                 `json:"listen_host"`
	ListenPort      string                 `json:"listen_port"`
	ReadOnly        bool                   `json:"read_only"`
	GremlinTimeout  int                    `json:"gremlin_timeout"`
	LoadSize        int                    `json:"load_size"`
}

var databasePath = flag.String("dbpath", "/tmp/testdb", "Path to the database.")
var databaseBackend = flag.String("db", "mem", "Database Backend.")
var host = flag.String("host", "0.0.0.0", "Host to listen on (defaults to all).")
var loadSize = flag.Int("load_size", 10000, "Size of triplesets to load")
var port = flag.String("port", "64210", "Port to listen on.")
var readOnly = flag.Bool("read_only", false, "Disable writing via HTTP.")
var gremlinTimeout = flag.Int("gremlin_timeout", 30, "Number of seconds until an individual query times out.")

func ParseConfigFromFile(filename string) *CayleyConfig {
	config := &CayleyConfig{}
	if filename == "" {
		return config
	}
	f, err := os.Open(filename)
	if err != nil {
		glog.Fatalln("Couldn't open config file", filename)
	}

	defer f.Close()

	dec := json.NewDecoder(f)
	err = dec.Decode(config)
	if err != nil {
		glog.Fatalln("Couldn't read config file:", err)
	}
	return config
}

func ParseConfigFromFlagsAndFile(fileFlag string) *CayleyConfig {
	// Find the file...
	var trueFilename string
	if fileFlag != "" {
		if _, err := os.Stat(fileFlag); os.IsNotExist(err) {
			glog.Fatalln("Cannot find specified configuration file", fileFlag, ", aborting.")
		} else {
			trueFilename = fileFlag
		}
	} else {
		if _, err := os.Stat(os.Getenv("CAYLEY_CFG")); err == nil {
			trueFilename = os.Getenv("CAYLEY_CFG")
		} else {
			if _, err := os.Stat("/etc/cayley.cfg"); err == nil {
				trueFilename = "/etc/cayley.cfg"
			}
		}
	}
	if trueFilename == "" {
		glog.Infoln("Couldn't find a config file in either $CAYLEY_CFG or /etc/cayley.cfg. Going by flag defaults only.")
	}
	config := ParseConfigFromFile(trueFilename)

	if config.DatabasePath == "" {
		config.DatabasePath = *databasePath
	}

	if config.DatabaseType == "" {
		config.DatabaseType = *databaseBackend
	}

	if config.ListenHost == "" {
		config.ListenHost = *host
	}

	if config.ListenPort == "" {
		config.ListenPort = *port
	}

	if config.GremlinTimeout == 0 {
		config.GremlinTimeout = *gremlinTimeout
	}

	if config.LoadSize == 0 {
		config.LoadSize = *loadSize
	}

	config.ReadOnly = config.ReadOnly || *readOnly

	return config
}
