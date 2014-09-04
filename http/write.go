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
	"io"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/barakmich/glog"
	"github.com/julienschmidt/httprouter"

	"github.com/google/cayley/quad"
	"github.com/google/cayley/quad/cquads"
)

func ParseJSONToQuadList(jsonBody []byte) ([]quad.Quad, error) {
	var quads []quad.Quad
	err := json.Unmarshal(jsonBody, &quads)
	if err != nil {
		return nil, err
	}
	for i, q := range quads {
		if !q.IsValid() {
			return nil, fmt.Errorf("invalid quad at index %d. %s", i, q)
		}
	}
	return quads, nil
}

func (api *API) ServeV1Write(w http.ResponseWriter, r *http.Request, _ httprouter.Params) int {
	if api.config.ReadOnly {
		return jsonResponse(w, 400, "Database is read-only.")
	}
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return jsonResponse(w, 400, err)
	}
	quads, err := ParseJSONToQuadList(bodyBytes)
	if err != nil {
		return jsonResponse(w, 400, err)
	}
	api.handle.QuadWriter.AddQuadSet(quads)
	fmt.Fprintf(w, "{\"result\": \"Successfully wrote %d quads.\"}", len(quads))
	return 200
}

func (api *API) ServeV1WriteNQuad(w http.ResponseWriter, r *http.Request, params httprouter.Params) int {
	if api.config.ReadOnly {
		return jsonResponse(w, 400, "Database is read-only.")
	}

	formFile, _, err := r.FormFile("NQuadFile")
	if err != nil {
		glog.Errorln(err)
		return jsonResponse(w, 500, "Couldn't read file: "+err.Error())
	}

	defer formFile.Close()

	blockSize, blockErr := strconv.ParseInt(r.URL.Query().Get("block_size"), 10, 64)
	if blockErr != nil {
		blockSize = int64(api.config.LoadSize)
	}

	// TODO(kortschak) Make this configurable from the web UI.
	dec := cquads.NewDecoder(formFile)

	var (
		n int

		block = make([]quad.Quad, 0, blockSize)
	)
	for {
		t, err := dec.Unmarshal()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic("what can do this here?") // FIXME(kortschak)
		}
		block = append(block, t)
		n++
		if len(block) == cap(block) {
			api.handle.QuadWriter.AddQuadSet(block)
			block = block[:0]
		}
	}
	api.handle.QuadWriter.AddQuadSet(block)

	fmt.Fprintf(w, "{\"result\": \"Successfully wrote %d quads.\"}", n)

	return 200
}

func (api *API) ServeV1Delete(w http.ResponseWriter, r *http.Request, params httprouter.Params) int {
	if api.config.ReadOnly {
		return jsonResponse(w, 400, "Database is read-only.")
	}
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return jsonResponse(w, 400, err)
	}
	quads, err := ParseJSONToQuadList(bodyBytes)
	if err != nil {
		return jsonResponse(w, 400, err)
	}
	count := 0
	for _, q := range quads {
		api.handle.QuadWriter.RemoveQuad(q)
		count++
	}
	fmt.Fprintf(w, "{\"result\": \"Successfully deleted %d quads.\"}", count)
	return 200
}
