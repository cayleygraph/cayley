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
	"github.com/google/cayley/graph/leveldb"
	"github.com/google/cayley/graph/memstore"
	"github.com/google/cayley/graph/mongo"
)

func Open(cfg *config.Config) graph.TripleStore {
	glog.Infof("Opening database \"%s\" at %s", cfg.DatabaseType, cfg.DatabasePath)
	switch cfg.DatabaseType {
	case "mongo", "mongodb":
		return mongo.NewTripleStore(cfg.DatabasePath, cfg.DatabaseOptions)
	case "leveldb":
		return leveldb.NewTripleStore(cfg.DatabasePath, cfg.DatabaseOptions)
	case "mem":
		ts := memstore.NewTripleStore()
		Load(ts, cfg, cfg.DatabasePath, true)
		return ts
	}
	panic("Unsupported database backend " + cfg.DatabaseType)
}
