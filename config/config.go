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
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/barakmich/glog"
)

type Config struct {
	DatabaseType    string
	DatabasePath    string
	DatabaseOptions map[string]interface{}
	ListenHost      string
	ListenPort      string
	ReadOnly        bool
	GremlinTimeout  time.Duration
	LoadSize        int
}

type config struct {
	DatabaseType    string                 `json:"database"`
	DatabasePath    string                 `json:"db_path"`
	DatabaseOptions map[string]interface{} `json:"db_options"`
	ListenHost      string                 `json:"listen_host"`
	ListenPort      string                 `json:"listen_port"`
	ReadOnly        bool                   `json:"read_only"`
	GremlinTimeout  duration               `json:"gremlin_timeout"`
	LoadSize        int                    `json:"load_size"`
}

func (c *Config) UnmarshalJSON(data []byte) error {
	var t config
	err := json.Unmarshal(data, &t)
	if err != nil {
		return err
	}
	*c = Config{
		DatabaseType:    t.DatabaseType,
		DatabasePath:    t.DatabasePath,
		DatabaseOptions: t.DatabaseOptions,
		ListenHost:      t.ListenHost,
		ListenPort:      t.ListenPort,
		ReadOnly:        t.ReadOnly,
		GremlinTimeout:  time.Duration(t.GremlinTimeout),
		LoadSize:        t.LoadSize,
	}
	return nil
}

func (c *Config) MarshalJSON() ([]byte, error) {
	return json.Marshal(config{
		DatabaseType:    c.DatabaseType,
		DatabasePath:    c.DatabasePath,
		DatabaseOptions: c.DatabaseOptions,
		ListenHost:      c.ListenHost,
		ListenPort:      c.ListenPort,
		ReadOnly:        c.ReadOnly,
		GremlinTimeout:  duration(c.GremlinTimeout),
		LoadSize:        c.LoadSize,
	})
}

// duration is a time.Duration that satisfies the
// json.UnMarshaler and json.Marshaler interfaces.
type duration time.Duration

// UnmarshalJSON unmarshals a duration according to the following scheme:
//  * If the element is absent the duration is zero.
//  * If the element is parsable as a time.Duration, the parsed value is kept.
//  * If the element is parsable as a number, that number of seconds is kept.
func (d *duration) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		*d = 0
		return nil
	}
	text := string(data)
	t, err := time.ParseDuration(text)
	if err == nil {
		*d = duration(t)
		return nil
	}
	i, err := strconv.ParseInt(text, 10, 64)
	if err == nil {
		*d = duration(time.Duration(i) * time.Second)
		return nil
	}
	// This hack is to get around strconv.ParseFloat
	// not handling e-notation for integers.
	f, err := strconv.ParseFloat(text, 64)
	*d = duration(time.Duration(f) * time.Second)
	return err
}

func (d *duration) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", *d)), nil
}

var (
	databasePath    = flag.String("dbpath", "/tmp/testdb", "Path to the database.")
	databaseBackend = flag.String("db", "memstore", "Database Backend.")
	host            = flag.String("host", "0.0.0.0", "Host to listen on (defaults to all).")
	loadSize        = flag.Int("load_size", 10000, "Size of triplesets to load")
	port            = flag.String("port", "64210", "Port to listen on.")
	readOnly        = flag.Bool("read_only", false, "Disable writing via HTTP.")
	gremlinTimeout  = flag.Duration("gremlin_timeout", 30*time.Second, "Elapsed time until an individual query times out.")
)

func ParseConfigFromFile(filename string) *Config {
	config := &Config{}
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

func ParseConfigFromFlagsAndFile(fileFlag string) *Config {
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
