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
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"

	"github.com/julienschmidt/httprouter"

	"github.com/cayleygraph/cayley/query"
)

type SuccessQueryWrapper struct {
	Result interface{} `json:"result"`
}

type ErrorQueryWrapper struct {
	Error string `json:"error"`
}

func WriteError(w io.Writer, err error) error {
	enc := json.NewEncoder(w)
	//enc.SetIndent("", " ")
	return enc.Encode(ErrorQueryWrapper{err.Error()})
}

func WriteResult(w io.Writer, result interface{}) error {
	enc := json.NewEncoder(w)
	//enc.SetIndent("", " ")
	return enc.Encode(SuccessQueryWrapper{result})
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

// ServeV1Query is the HTTP handler for queries in API V1
func (api *API) ServeV1Query(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	ctx, cancel := api.contextForRequest(r)
	defer cancel()
	l := query.GetLanguage(params.ByName("query_lang"))
	if l == nil {
		jsonResponse(w, http.StatusBadRequest, "Unknown query language.")
		return
	}
	errFunc := defaultErrorFunc
	if l.HTTPError != nil {
		errFunc = l.HTTPError
	}
	select {
	case <-ctx.Done():
		errFunc(w, ctx.Err())
		return
	default:
	}
	h, err := api.GetHandleForRequest(r)
	if err != nil {
		errFunc(w, err)
		return
	}
	if l.HTTPQuery != nil {
		defer r.Body.Close()
		l.HTTPQuery(ctx, h.QuadStore, w, r.Body)
		return
	}
	if l.Session == nil {
		errFunc(w, errors.New("no support for HTTP interface for this query language"))
		return
	}

	par, _ := url.ParseQuery(r.URL.RawQuery)
	limit, _ := strconv.Atoi(par.Get("limit"))
	if limit == 0 {
		limit = 100
	}

	ses := l.Session(h.QuadStore)
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		errFunc(w, err)
		return
	}
	it, err := ses.Execute(ctx, string(bodyBytes), query.Options{
		Collation: query.JSON,
		Limit:     limit,
	})
	if err != nil {
		errFunc(w, err)
		return
	}
	defer it.Close()

	var out []interface{}
	for it.Next(ctx) {
		out = append(out, it.Result())
	}
	if err = it.Err(); err != nil {
		errFunc(w, err)
		return
	}
	_ = WriteResult(w, out)
}

func (api *API) ServeV1Shape(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	jsonResponse(w, http.StatusNotImplemented, "Query shape API v1 is deprecated.")
}
