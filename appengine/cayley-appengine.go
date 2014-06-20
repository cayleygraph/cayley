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

package cayleyappengine

import (
	"cayley_config"
	"cayley_http"
	"github.com/barakmich/glog"
	"graph"
	"graph_memstore"
	"nquads"
	"os"
)

func init() {
	glog.SetToStderr(true)
	config := cayley_config.ParseConfigFromFile("cayley_appengine.cfg")
	ts := graph_memstore.NewMemTripleStore()
	glog.Errorln(config)
	LoadTriplesFromFileInto(ts, config.DatabasePath, config.LoadSize)
	cayley_http.SetupRoutes(ts, config)
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

func LoadTriplesFromFileInto(ts graph.TripleStore, filename string, loadSize int) {
	tChan := make(chan *graph.Triple)
	go ReadTriplesFromFile(tChan, filename)
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
