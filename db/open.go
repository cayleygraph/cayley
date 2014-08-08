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

package db

import (
	"github.com/barakmich/glog"

	"github.com/google/cayley/config"
	"github.com/google/cayley/graph"
)

func Open(cfg *config.Config) (graph.TripleStore, error) {
	glog.Infof("Opening database %q at %s", cfg.DatabaseType, cfg.DatabasePath)
	ts, err := graph.NewTripleStore(cfg.DatabaseType, cfg.DatabasePath, cfg.DatabaseOptions)
	if err != nil {
		return nil, err
	}

	return ts, nil
}
