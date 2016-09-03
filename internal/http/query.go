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
	"errors"
	"io/ioutil"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"golang.org/x/net/context"

	"github.com/cayleygraph/cayley/query"
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

func GetQueryShape(q string, ses query.HTTP) ([]byte, error) {
	s, err := ses.ShapeOf(q)
	if err != nil {
		return nil, err
	}
	return json.Marshal(s)
}

func (api *API) contextForRequest(r *http.Request) (context.Context, func()) {
	ctx := context.TODO() // TODO(dennwc): get from request
	cancel := func() {}
	if api.config.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, api.config.Timeout)
	}
	return ctx, cancel
}

func defaultErrorFunc(w query.ResponseWriter, err error) {
	data, _ := json.Marshal(err.Error())
	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte(`{"error" : `))
	w.Write(data)
	w.Write([]byte(`}`))
}

// TODO(barakmich): Turn this into proper middleware.
func (api *API) ServeV1Query(w http.ResponseWriter, r *http.Request, params httprouter.Params) int {
	ctx, cancel := api.contextForRequest(r)
	defer cancel()
	l := query.GetLanguage(params.ByName("query_lang"))
	if l == nil {
		return jsonResponse(w, 400, "Unknown query language.")
	}
	errFunc := defaultErrorFunc
	if l.HTTPError != nil {
		errFunc = l.HTTPError
	}
	select {
	case <-ctx.Done():
		errFunc(w, ctx.Err())
		return 0
	default:
	}
	h, err := api.GetHandleForRequest(r)
	if err != nil {
		errFunc(w, err)
		return 400
	}
	if l.HTTPQuery != nil {
		defer r.Body.Close()
		l.HTTPQuery(ctx, h.QuadStore, w, r.Body)
		return 0
	}
	if l.HTTP == nil {
		errFunc(w, errors.New("HTTP interface is not supported for this query language."))
		return 400
	}
	ses := l.HTTP(h.QuadStore)
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		errFunc(w, err)
		return 400
	}
	code := string(bodyBytes)

	c := make(chan query.Result, 5)
	go ses.Execute(ctx, code, c, 100)

	for res := range c {
		if err := res.Err(); err != nil {
			if err == nil {
				continue // wait for results channel to close
			}
			errFunc(w, err)
			return 400
		}
		ses.Collate(res)
	}
	output, err := ses.Results()
	if err != nil {
		errFunc(w, err)
		return 400
	}
	bytes, err := WrapResult(output)
	if err != nil {
		errFunc(w, err)
		return 400
	}
	w.Write(bytes)
	return 200
}

func (api *API) ServeV1Shape(w http.ResponseWriter, r *http.Request, params httprouter.Params) int {
	ctx, cancel := api.contextForRequest(r)
	defer cancel()
	select {
	case <-ctx.Done():
		return jsonResponse(w, 400, "Cancelled")
	default:
	}
	h, err := api.GetHandleForRequest(r)
	if err != nil {
		return jsonResponse(w, 400, err)
	}
	l := query.GetLanguage(params.ByName("query_lang"))
	if l == nil {
		return jsonResponse(w, 400, "Unknown query language.")
	} else if l.HTTP == nil {
		return jsonResponse(w, 400, "HTTP interface is not supported for this query language.")
	}
	ses := l.HTTP(h.QuadStore)
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return jsonResponse(w, 400, err)
	}
	code := string(bodyBytes)

	output, err := GetQueryShape(code, ses)
	if err == query.ErrParseMore {
		return jsonResponse(w, 500, "Incomplete data?")
	} else if err != nil {
		bytes, _ := WrapErrResult(err)
		http.Error(w, string(bytes), 400)
		return 400
	}
	w.Write(output)
	return 200
}
