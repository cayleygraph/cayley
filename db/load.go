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
	"os"

	"github.com/barakmich/glog"

	"github.com/google/cayley/config"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/nquads"
)

type bulkLoadable interface {
	// BulkLoad loads Triples from a channel in bulk to the TripleStore. It
	// returns false if bulk loading is not possible (i.e. if you cannot load
	// in bulk to a non-empty database, and the current database is non-empty)
	BulkLoad(chan *graph.Triple) bool
}

func Load(ts graph.TripleStore, cfg *config.Config, triplePath string) {
	tChan := make(chan *graph.Triple)
	go ReadTriplesFromFile(tChan, triplePath)

	bulker, canBulk := ts.(bulkLoadable)
	if canBulk && bulker.BulkLoad(tChan) {
		return
	}

	LoadTriplesInto(tChan, ts, cfg.LoadSize)
}

func ReadTriplesFromFile(c chan *graph.Triple, tripleFile string) {
	f, err := os.Open(tripleFile)
	if err != nil {
		glog.Fatalln("Couldn't open file", tripleFile)
	}

	defer func() {
		if err := f.Close(); err != nil {
			glog.Fatalln(err)
		}
	}()

	nquads.ReadNQuadsFromReader(c, f)
}

func LoadTriplesInto(tChan chan *graph.Triple, ts graph.TripleStore, loadSize int) {
	tripleblock := make([]*graph.Triple, loadSize)
	i := 0
	for t := range tChan {
		tripleblock[i] = t
		i++
		if i == loadSize {
			ts.AddTripleSet(tripleblock)
			i = 0
		}
	}
	ts.AddTripleSet(tripleblock[0:i])
}
