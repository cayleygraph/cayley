// Copyright 2017 The Cayley Authors. All rights reserved.
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

package cayleyhttp

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
	_ "github.com/cayleygraph/cayley/writer"
)

func NewAPIv2(h *graph.Handle) *APIv2 {
	return NewAPIv2Writer(h, "single", nil)
}

func NewAPIv2Writer(h *graph.Handle, wtype string, wopts graph.Options) *APIv2 {
	api := &APIv2{h: h, wtyp: wtype, wopt: wopts}
	api.r = httprouter.New()
	api.RegisterOn(api.r)
	return api
}

type APIv2 struct {
	h     *graph.Handle
	r     *httprouter.Router
	ro    bool
	batch int
	wtyp  string
	wopt  graph.Options
}

func (api *APIv2) SetReadOnly(ro bool) {
	api.ro = ro
}
func (api *APIv2) SetBatchSize(n int) {
	api.batch = n
}
func (api *APIv2) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	api.r.ServeHTTP(w, r)
}

type HandlerWrapper func(httprouter.Handle) httprouter.Handle

func wrap(h http.HandlerFunc, arr []HandlerWrapper) httprouter.Handle {
	wh := func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		h(w, r)
	}
	for _, w := range arr {
		wh = w(wh)
	}
	return wh
}
func (api *APIv2) RegisterOn(r *httprouter.Router, wrappers ...HandlerWrapper) {
	if !api.ro {
		r.POST("/api/v2/write", wrap(api.ServeWrite, wrappers))
		r.POST("/api/v2/delete", wrap(api.ServeDelete, wrappers))
	}
	r.POST("/api/v2/read", wrap(api.ServeRead, wrappers))
	r.GET("/api/v2/read", wrap(api.ServeRead, wrappers))
	r.GET("/api/v2/formats", wrap(api.ServeFormats, wrappers))
}

const (
	defaultFormat      = "nquads"
	hdrContentType     = "Content-Type"
	hdrContentEncoding = "Content-Encoding"
	hdrAccept          = "Accept"
	hdrAcceptEncoding  = "Accept-Encoding"
	contentTypeJSON    = "application/json"
)

func getFormat(r *http.Request, formKey string, acceptName string) *quad.Format {
	var format *quad.Format
	if formKey != "" {
		if name := r.FormValue("format"); name != "" {
			format = quad.FormatByName(name)
		}
	}
	if acceptName != "" && format == nil {
		specs := ParseAccept(r.Header, acceptName)
		// TODO: sort by Q
		if len(specs) != 0 {
			format = quad.FormatByMime(specs[0].Value)
		}
	}
	if format == nil {
		format = quad.FormatByName(defaultFormat)
	}
	return format
}

func readerFrom(r *http.Request, acceptName string) (io.ReadCloser, error) {
	if specs := ParseAccept(r.Header, acceptName); len(specs) != 0 {
		if s := specs[0]; s.Value == "gzip" {
			zr, err := gzip.NewReader(r.Body)
			if err != nil {
				return nil, err
			}
			return zr, nil
		}
	}
	return r.Body, nil
}

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error { return nil }

func writerFrom(w http.ResponseWriter, r *http.Request, acceptName string) io.WriteCloser {
	if specs := ParseAccept(r.Header, acceptName); len(specs) != 0 {
		if s := specs[0]; s.Value == "gzip" {
			w.Header().Set(hdrContentEncoding, s.Value)
			zw := gzip.NewWriter(w)
			return zw
		}
	}
	return nopWriteCloser{Writer: w}
}

func (api *APIv2) handleForRequest(r *http.Request) (*graph.Handle, error) {
	return HandleForRequest(api.h, api.wtyp, api.wopt, r)
}

func (api *APIv2) ServeWrite(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	format := getFormat(r, "", hdrContentType)
	if format == nil || format.Reader == nil {
		jsonResponse(w, http.StatusBadRequest, errors.New("format is not supported for reading data"))
		return
	}
	rd, err := readerFrom(r, hdrContentEncoding)
	if err != nil {
		jsonResponse(w, http.StatusBadRequest, err)
		return
	}
	defer rd.Close()
	qr := format.Reader(rd)
	defer qr.Close()
	h, err := api.handleForRequest(r)
	if err != nil {
		jsonResponse(w, http.StatusBadRequest, err)
		return
	}
	qw := graph.NewWriter(h.QuadWriter)
	defer qw.Close()
	n, err := quad.CopyBatch(qw, qr, api.batch)
	if err != nil {
		jsonResponse(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set(hdrContentType, contentTypeJSON)
	fmt.Fprintf(w, `{"result": "Successfully wrote %d quads.", "count": %d}`+"\n", n, n)
}

func (api *APIv2) ServeDelete(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	format := getFormat(r, "", hdrContentType)
	if format == nil || format.Reader == nil {
		jsonResponse(w, http.StatusBadRequest, fmt.Errorf("format is not supported for reading data"))
		return
	}
	rd, err := readerFrom(r, hdrContentEncoding)
	if err != nil {
		jsonResponse(w, http.StatusBadRequest, err)
		return
	}
	defer rd.Close()
	qr := format.Reader(r.Body)
	defer qr.Close()
	h, err := api.handleForRequest(r)
	if err != nil {
		jsonResponse(w, http.StatusBadRequest, err)
		return
	}
	qw := graph.NewRemover(h.QuadWriter)
	defer qw.Close()
	n, err := quad.CopyBatch(qw, qr, api.batch)
	if err != nil {
		jsonResponse(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set(hdrContentType, contentTypeJSON)
	fmt.Fprintf(w, `{"result": "Successfully deleted %d quads.", "count": %d}`+"\n", n, n)
}

type checkWriter struct {
	w       io.Writer
	written bool
}

func (w *checkWriter) Write(p []byte) (int, error) {
	w.written = true
	return w.w.Write(p)
}

func (api *APIv2) ServeRead(w http.ResponseWriter, r *http.Request) {
	format := getFormat(r, "format", hdrAccept)
	if format == nil || format.Writer == nil {
		jsonResponse(w, http.StatusBadRequest, fmt.Errorf("format is not supported for reading data"))
		return
	}
	h, err := api.handleForRequest(r)
	if err != nil {
		jsonResponse(w, http.StatusBadRequest, err)
		return
	}
	qr := graph.NewQuadStoreReader(h.QuadStore)
	defer qr.Close()

	wr := writerFrom(w, r, hdrAcceptEncoding)
	defer wr.Close()

	cw := &checkWriter{w: wr}
	qw := format.Writer(cw)
	defer qw.Close()
	if len(format.Mime) != 0 {
		w.Header().Set(hdrContentType, format.Mime[0])
	}
	if bw, ok := qw.(quad.BatchWriter); ok {
		_, err = quad.CopyBatch(bw, qr, api.batch)
	} else {
		_, err = quad.Copy(qw, qr)
	}
	if err != nil && !cw.written {
		jsonResponse(w, http.StatusInternalServerError, err)
		return
	} else if err != nil {
		// can do nothing here, since first byte (and header) was written
		// TODO: check if client just gone away
		clog.Errorf("read quads error: %v", err)
	}
}

func (api *APIv2) ServeFormats(w http.ResponseWriter, r *http.Request) {
	type Format struct {
		Id     string   `json:"id"`
		Read   bool     `json:"read,omitempty"`
		Write  bool     `json:"write,omitempty"`
		Ext    []string `json:"ext,omitempty"`
		Mime   []string `json:"mime,omitempty"`
		Binary bool     `json:"binary,omitempty"`
	}
	formats := quad.Formats()
	out := make([]Format, 0, len(formats))
	for _, f := range formats {
		out = append(out, Format{
			Id:  f.Name,
			Ext: f.Ext, Mime: f.Mime,
			Read: f.Reader != nil, Write: f.Writer != nil,
			Binary: f.Binary,
		})
	}
	w.Header().Set(hdrContentType, contentTypeJSON)
	json.NewEncoder(w).Encode(out)
}
