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

	"github.com/julienschmidt/httprouter"

	"github.com/google/cayley/query"
	"github.com/google/cayley/query/gremlin"
	"github.com/google/cayley/query/mql"
)

type SuccessQueryWrapper struct {
	Result interface{} `json:"result"`
}

type ErrorQueryWrapper struct {
	Error string `json:"error"`
}

func WrapErrResult(err error) ([]byte, error) {
	var wrap ErrorQueryWrapper
	wrap.Error = err.Error()
	return json.MarshalIndent(wrap, "", " ")
}

func WrapResult(result interface{}) ([]byte, error) {
	var wrap SuccessQueryWrapper
	wrap.Result = result
	return json.MarshalIndent(wrap, "", " ")
}

func Run(q string, ses query.HTTP) (interface{}, error) {
	c := make(chan interface{}, 5)
	go ses.Execute(q, c, 100)
	for res := range c {
		ses.Collate(res)
	}
	return ses.Results()
}

func GetQueryShape(q string, ses query.HTTP) ([]byte, error) {
	s, err := ses.ShapeOf(q)
	if err != nil {
		return nil, err
	}
	return json.Marshal(s)
}

// TODO(barakmich): Turn this into proper middleware.
func (api *API) ServeV1Query(w http.ResponseWriter, r *http.Request, params httprouter.Params) int {
	h, err := api.GetHandleForRequest(r)
	var ses query.HTTP
	switch params.ByName("query_lang") {
	case "gremlin":
		ses = gremlin.NewSession(h.QuadStore, api.config.Timeout, false)
	case "mql":
		ses = mql.NewSession(h.QuadStore)
	default:
		return jsonResponse(w, 400, "Need a query language.")
	}
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return jsonResponse(w, 400, err)
	}
	code := string(bodyBytes)
	result, err := ses.Parse(code)
	switch result {
	case query.Parsed:
		var output interface{}
		var bytes []byte
		var err error
		output, err = Run(code, ses)
		if err != nil {
			bytes, err = WrapErrResult(err)
			http.Error(w, string(bytes), 400)
			ses = nil
			return 400
		}
		bytes, err = WrapResult(output)
		if err != nil {
			ses = nil
			return jsonResponse(w, 400, err)
		}
		fmt.Fprint(w, string(bytes))
		ses = nil
		return 200
	case query.ParseFail:
		ses = nil
		return jsonResponse(w, 400, err)
	default:
		ses = nil
		return jsonResponse(w, 500, "Incomplete data?")
	}
}

func (api *API) ServeV1Shape(w http.ResponseWriter, r *http.Request, params httprouter.Params) int {
	h, err := api.GetHandleForRequest(r)
	var ses query.HTTP
	switch params.ByName("query_lang") {
	case "gremlin":
		ses = gremlin.NewSession(h.QuadStore, api.config.Timeout, false)
	case "mql":
		ses = mql.NewSession(h.QuadStore)
	default:
		return jsonResponse(w, 400, "Need a query language.")
	}
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return jsonResponse(w, 400, err)
	}
	code := string(bodyBytes)
	result, err := ses.Parse(code)
	switch result {
	case query.Parsed:
		var output []byte
		var err error
		output, err = GetQueryShape(code, ses)
		if err != nil {
			return jsonResponse(w, 400, err)
		}
		fmt.Fprint(w, string(output))
		return 200
	case query.ParseFail:
		return jsonResponse(w, 400, err)
	default:
		return jsonResponse(w, 500, "Incomplete data?")
	}
}
