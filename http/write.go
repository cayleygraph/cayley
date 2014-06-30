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

package http

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/barakmich/glog"
	"github.com/julienschmidt/httprouter"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/nquads"
)

func ParseJsonToTripleList(jsonBody []byte) ([]*graph.Triple, error) {
	var tripleList []*graph.Triple
	err := json.Unmarshal(jsonBody, &tripleList)
	if err != nil {
		return nil, err
	}
	for i, t := range tripleList {
		if !t.IsValid() {
			return nil, fmt.Errorf("Invalid triple at index %d. %s", i, t)
		}
	}
	return tripleList, nil
}

func (api *Api) ServeV1Write(w http.ResponseWriter, r *http.Request, _ httprouter.Params) int {
	if api.config.ReadOnly {
		return FormatJson400(w, "Database is read-only.")
	}
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return FormatJson400(w, err)
	}
	tripleList, terr := ParseJsonToTripleList(bodyBytes)
	if terr != nil {
		return FormatJson400(w, terr)
	}
	api.ts.AddTripleSet(tripleList)
	fmt.Fprintf(w, "{\"result\": \"Successfully wrote %d triples.\"}", len(tripleList))
	return 200
}

func (api *Api) ServeV1WriteNQuad(w http.ResponseWriter, r *http.Request, params httprouter.Params) int {
	if api.config.ReadOnly {
		return FormatJson400(w, "Database is read-only.")
	}

	formFile, _, err := r.FormFile("NQuadFile")
	if err != nil {
		glog.Errorln(err)
		return FormatJsonError(w, 500, "Couldn't read file: "+err.Error())
	}

	defer formFile.Close()

	blockSize, blockErr := strconv.ParseInt(r.URL.Query().Get("block_size"), 10, 64)
	if blockErr != nil {
		blockSize = int64(api.config.LoadSize)
	}

	tChan := make(chan *graph.Triple)
	go nquads.ReadNQuadsFromReader(tChan, formFile)
	tripleblock := make([]*graph.Triple, blockSize)
	nTriples := 0
	i := int64(0)
	for t := range tChan {
		tripleblock[i] = t
		i++
		nTriples++
		if i == blockSize {
			api.ts.AddTripleSet(tripleblock)
			i = 0
		}
	}
	api.ts.AddTripleSet(tripleblock[0:i])
	fmt.Fprintf(w, "{\"result\": \"Successfully wrote %d triples.\"}", nTriples)
	return 200
}

func (api *Api) ServeV1Delete(w http.ResponseWriter, r *http.Request, params httprouter.Params) int {
	if api.config.ReadOnly {
		return FormatJson400(w, "Database is read-only.")
	}
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return FormatJson400(w, err)
	}
	tripleList, terr := ParseJsonToTripleList(bodyBytes)
	if terr != nil {
		return FormatJson400(w, terr)
	}
	count := 0
	for _, triple := range tripleList {
		api.ts.RemoveTriple(triple)
		count++
	}
	fmt.Fprintf(w, "{\"result\": \"Successfully deleted %d triples.\"}", count)
	return 200
}
