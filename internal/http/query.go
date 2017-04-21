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
	"io"
)

type SuccessQueryWrapper struct {
	Result interface{} `json:"result"`
}

type ErrorQueryWrapper struct {
	Error string `json:"error"`
}

func WriteError(w io.Writer, err error) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", " ")
	return enc.Encode(ErrorQueryWrapper{err.Error()})
}

func WriteResult(w io.Writer, result interface{}) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", " ")
	return enc.Encode(SuccessQueryWrapper{result})
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
	if l.HTTP == nil {
		errFunc(w, errors.New("HTTP interface is not supported for this query language."))
		return
	}
	ses := l.HTTP(h.QuadStore)
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		errFunc(w, err)
		return
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
			return
		}
		ses.Collate(res)
	}
	output, err := ses.Results()
	if err != nil {
		errFunc(w, err)
		return
	}
	_ = WriteResult(w, output)
}

func (api *API) ServeV1Shape(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	ctx, cancel := api.contextForRequest(r)
	defer cancel()
	select {
	case <-ctx.Done():
		jsonResponse(w, http.StatusBadRequest, "Cancelled")
		return
	default:
	}
	h, err := api.GetHandleForRequest(r)
	if err != nil {
		jsonResponse(w, http.StatusBadRequest, err)
		return
	}
	l := query.GetLanguage(params.ByName("query_lang"))
	if l == nil {
		jsonResponse(w, http.StatusBadRequest, "Unknown query language.")
		return
	} else if l.HTTP == nil {
		jsonResponse(w, http.StatusBadRequest, "HTTP interface is not supported for this query language.")
		return
	}
	ses := l.HTTP(h.QuadStore)
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		jsonResponse(w, http.StatusBadRequest, err)
		return
	}
	code := string(bodyBytes)

	output, err := GetQueryShape(code, ses)
	if err == query.ErrParseMore {
		jsonResponse(w, http.StatusBadRequest, "Incomplete data?")
		return
	} else if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		WriteError(w, err)
		return
	}
	w.Write(output)
}
