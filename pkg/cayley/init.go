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

package cayley

import (
	cfg "github.com/google/cayley/pkg/cayley/config"
	graph_leveldb "github.com/google/cayley/pkg/graph/leveldb"
	graph_mongo "github.com/google/cayley/pkg/graph/mongo"
)

func CayleyInit(config *cfg.CayleyConfig, triplePath string) bool {
	created := false
	dbpath := config.DatabasePath
	switch config.DatabaseType {
	case "mongo", "mongodb":
		created = graph_mongo.CreateNewMongoGraph(dbpath, config.DatabaseOptions)
	case "leveldb":
		created = graph_leveldb.CreateNewLevelDB(dbpath)
	case "mem":
		return true
	}
	if created && triplePath != "" {
		ts := OpenTSFromConfig(config)
		CayleyLoad(ts, config, triplePath, true)
		ts.Close()
	}
	return created
}
