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
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config defines the behavior of cayley database instances.
type Config struct {
	DatabaseType               string
	DatabasePath               string
	DatabaseOptions            map[string]interface{}
	ReplicationType            string
	ReplicationOptions         map[string]interface{}
	ListenHost                 string
	ListenPort                 string
	ReadOnly                   bool
	Timeout                    time.Duration
	LoadSize                   int
	RequiresHTTPRequestContext bool
}

type config struct {
	DatabaseType               string                 `json:"database"`
	DatabasePath               string                 `json:"db_path"`
	DatabaseOptions            map[string]interface{} `json:"db_options"`
	ReplicationType            string                 `json:"replication"`
	ReplicationOptions         map[string]interface{} `json:"replication_options"`
	ListenHost                 string                 `json:"listen_host"`
	ListenPort                 string                 `json:"listen_port"`
	ReadOnly                   bool                   `json:"read_only"`
	Timeout                    duration               `json:"timeout"`
	LoadSize                   int                    `json:"load_size"`
	RequiresHTTPRequestContext bool                   `json:"http_request_context"`
}

func (c *Config) UnmarshalJSON(data []byte) error {
	var t config
	err := json.Unmarshal(data, &t)
	if err != nil {
		return err
	}
	*c = Config{
		DatabaseType:               t.DatabaseType,
		DatabasePath:               t.DatabasePath,
		DatabaseOptions:            t.DatabaseOptions,
		ReplicationType:            t.ReplicationType,
		ReplicationOptions:         t.ReplicationOptions,
		ListenHost:                 t.ListenHost,
		ListenPort:                 t.ListenPort,
		ReadOnly:                   t.ReadOnly,
		Timeout:                    time.Duration(t.Timeout),
		LoadSize:                   t.LoadSize,
		RequiresHTTPRequestContext: t.RequiresHTTPRequestContext,
	}
	return nil
}

func (c *Config) MarshalJSON() ([]byte, error) {
	return json.Marshal(config{
		DatabaseType:       c.DatabaseType,
		DatabasePath:       c.DatabasePath,
		DatabaseOptions:    c.DatabaseOptions,
		ReplicationType:    c.ReplicationType,
		ReplicationOptions: c.ReplicationOptions,
		ListenHost:         c.ListenHost,
		ListenPort:         c.ListenPort,
		ReadOnly:           c.ReadOnly,
		Timeout:            duration(c.Timeout),
		LoadSize:           c.LoadSize,
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

// Load reads a JSON-encoded config contained in the given file. A zero value
// config is returned if the filename is empty.
func Load(file string) (*Config, error) {
	config := &Config{}
	if file == "" {
		return config, nil
	}
	f, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("could not open config file %q: %v", file, err)
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	err = dec.Decode(config)
	if err != nil {
		return nil, fmt.Errorf("could not parse config file %q: %v", file, err)
	}
	return config, nil
}
