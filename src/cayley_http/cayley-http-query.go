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

package cayley_http

import (
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"graph"
	"gremlin"
	"io/ioutil"
	"mql"
	"net/http"
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

func RunJsonQuery(query string, ses graph.HttpSession) (interface{}, error) {
	c := make(chan interface{}, 5)

	go ses.ExecInput(query, c, 100)

	for res := range c {
		ses.BuildJson(res)
	}

	return ses.GetJson()
}

func GetQueryShape(query string, ses graph.HttpSession) ([]byte, error) {
	c := make(chan map[string]interface{}, 5)

	go ses.GetQuery(query, c)

	var data map[string]interface{}

	for res := range c {
		data = res
	}

	return json.Marshal(data)
}

// TODO(barakmich): Turn this into proper middleware.
func (api *Api) ServeV1Query(w http.ResponseWriter, r *http.Request, params httprouter.Params) int {
	var ses graph.HttpSession

	switch params.ByName("query_lang") {
	case "gremlin":
		ses = gremlin.NewGremlinSession(api.ts, api.config.GremlinTimeout, false)

	case "mql":
		ses = mql.NewMqlSession(api.ts)

	default:
		return FormatJson400(w, "Need a query language.")
	}

	bodyBytes, err := ioutil.ReadAll(r.Body)

	if err != nil {
		return FormatJson400(w, err)
	}

	code := string(bodyBytes)
	result, err := ses.InputParses(code)

	switch result {
	case graph.Parsed:
		var bytes []byte

		output, err := RunJsonQuery(code, ses)

		if err != nil {
			bytes, _ := WrapErrResult(err)

			http.Error(w, string(bytes), 400)

			ses = nil

			return 400
		}

		bytes, err = WrapResult(output)

		if err != nil {
			ses = nil

			return FormatJson400(w, err)
		}

		fmt.Fprint(w, string(bytes))

		ses = nil

		return 200

	case graph.ParseFail:
		ses = nil

		return FormatJson400(w, err)

	default:
		ses = nil

		return FormatJsonError(w, 500, "Incomplete data?")
	}

	http.Error(w, "", http.StatusNotFound)

	ses = nil

	return http.StatusNotFound
}

func (api *Api) ServeV1Shape(w http.ResponseWriter, r *http.Request, params httprouter.Params) int {
	var ses graph.HttpSession

	switch params.ByName("query_lang") {
	case "gremlin":
		ses = gremlin.NewGremlinSession(api.ts, api.config.GremlinTimeout, false)

	case "mql":
		ses = mql.NewMqlSession(api.ts)

	default:
		return FormatJson400(w, "Need a query language.")
	}

	bodyBytes, err := ioutil.ReadAll(r.Body)

	if err != nil {
		return FormatJson400(w, err)
	}

	code := string(bodyBytes)
	result, err := ses.InputParses(code)

	switch result {
	case graph.Parsed:
		output, err := GetQueryShape(code, ses)

		if err != nil {
			return FormatJson400(w, err)
		}

		fmt.Fprint(w, string(output))

		return 200

	case graph.ParseFail:
		return FormatJson400(w, err)

	default:
		return FormatJsonError(w, 500, "Incomplete data?")
	}

	http.Error(w, "", http.StatusNotFound)

	return http.StatusNotFound
}
