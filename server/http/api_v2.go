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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/shape"
	_ "github.com/cayleygraph/cayley/writer"
	"github.com/cayleygraph/quad"
)

const (
	prefix             = "/api/v2"
	defaultLimit       = 100
	defaultReplication = "single"
)

// NewAPIv2 creates a new instance of APIv2 with default options
func NewAPIv2(h *graph.Handle, wrappers ...HandlerWrapper) *APIv2 {
	return NewAPIv2Writer(h, defaultReplication, nil, wrappers...)
}

// NewBoundAPIv2 creates a new instance of APIv2 bound to a given httprouter.Router
func NewBoundAPIv2(h *graph.Handle, r *httprouter.Router) *APIv2 {
	api := &APIv2{h: h, wtyp: defaultReplication, wopt: nil, limit: defaultLimit, handler: r}
	api.registerOn(r)
	return api
}

// NewAPIv2Writer creates a new instance of APIv2
func NewAPIv2Writer(h *graph.Handle, wtype string, wopts graph.Options, wrappers ...HandlerWrapper) *APIv2 {
	r := httprouter.New()
	api := &APIv2{h: h, wtyp: wtype, wopt: wopts, limit: defaultLimit}
	api.registerOn(r)
	var handler http.Handler = r
	for _, wrapper := range wrappers {
		handler = wrapper(handler)
	}
	api.handler = handler
	return api
}

type APIv2 struct {
	h       *graph.Handle
	ro      bool
	batch   int
	handler http.Handler

	// replication
	wtyp string
	wopt graph.Options

	// query
	timeout time.Duration
	limit   int
}

func (api *APIv2) SetReadOnly(ro bool) {
	api.ro = ro
}
func (api *APIv2) SetBatchSize(n int) {
	api.batch = n
}
func (api *APIv2) SetQueryTimeout(dt time.Duration) {
	api.timeout = dt
}
func (api *APIv2) SetQueryLimit(n int) {
	api.limit = n
}

func (api *APIv2) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	api.handler.ServeHTTP(w, r)
}

type HandlerWrapper func(http.Handler) http.Handler

func toHandle(handler http.HandlerFunc) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		handler(w, r)
	}
}

func (api *APIv2) registerDataOn(r *httprouter.Router) {
	if !api.ro {
		r.POST(prefix+"/write", toHandle(api.ServeWrite))
		r.POST(prefix+"/delete", toHandle(api.ServeDelete))
		r.POST(prefix+"/node/delete", toHandle(api.ServeNodeDelete))
	}
	r.POST(prefix+"/read", toHandle(api.ServeRead))
	r.GET(prefix+"/read", toHandle(api.ServeRead))
	r.GET(prefix+"/formats", toHandle(api.ServeFormats))
}

func (api *APIv2) registerQueryOn(r *httprouter.Router) {
	r.POST(prefix+"/query", toHandle(api.ServeQuery))
	r.GET(prefix+"/query", toHandle(api.ServeQuery))
}

func (api *APIv2) registerOn(r *httprouter.Router) {
	api.registerDataOn(r)
	api.registerQueryOn(r)
}

const (
	defaultFormat      = "nquads"
	hdrContentType     = "Content-Type"
	hdrContentEncoding = "Content-Encoding"
	hdrAccept          = "Accept"
	hdrAcceptEncoding  = "Accept-Encoding"
	contentTypeJSON    = "application/json"
	contentTypeJSONLD  = "application/ld+json"
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

// writeResponse represents the response received for a successful write
type writeResponse struct {
	Result string `json:"result"`
	Count  int    `json:"count"`
}

// newWriteResponse creates a new WriteResponse for given count of quads written
func newWriteResponse(count int) writeResponse {
	return writeResponse{
		Result: fmt.Sprintf("Successfully wrote %d quads.", count),
		Count:  count,
	}
}

func (api *APIv2) ServeWrite(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if api.ro {
		jsonResponse(w, http.StatusForbidden, errors.New("database is read-only"))
		return
	}
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
	err = qw.Close()
	if err != nil {
		jsonResponse(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set(hdrContentType, contentTypeJSON)
	response := newWriteResponse(n)
	encoder := json.NewEncoder(w)
	encoder.Encode(response)
}

func (api *APIv2) ServeDelete(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if api.ro {
		jsonResponse(w, http.StatusForbidden, errors.New("database is read-only"))
		return
	}
	format := getFormat(r, "", hdrContentType)
	if format == nil || format.Reader == nil {
		jsonResponse(w, http.StatusBadRequest, fmt.Errorf("format is not supported for reading quads"))
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

func (api *APIv2) ServeNodeDelete(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if api.ro {
		jsonResponse(w, http.StatusForbidden, errors.New("database is read-only"))
		return
	}
	format := getFormat(r, "", hdrContentType)
	if format == nil || format.UnmarshalValue == nil {
		jsonResponse(w, http.StatusBadRequest, fmt.Errorf("format is not supported for reading nodes"))
		return
	}
	const limit = 128*1024 + 1
	rd := io.LimitReader(r.Body, limit)
	data, err := ioutil.ReadAll(rd)
	if err != nil {
		jsonResponse(w, http.StatusBadRequest, err)
		return
	} else if len(data) == limit {
		jsonResponse(w, http.StatusBadRequest, fmt.Errorf("request data is too large"))
		return
	}
	v, err := format.UnmarshalValue(data)
	if err != nil {
		jsonResponse(w, http.StatusBadRequest, err)
		return
	} else if v == nil {
		jsonResponse(w, http.StatusBadRequest, fmt.Errorf("cannot remove nil value"))
		return
	}
	h, err := api.handleForRequest(r)
	if err != nil {
		jsonResponse(w, http.StatusBadRequest, err)
		return
	}
	err = h.RemoveNode(v)
	if err != nil {
		jsonResponse(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set(hdrContentType, contentTypeJSON)
	const n = 1
	fmt.Fprintf(w, `{"result": "Successfully deleted %d nodes.", "count": %d}`+"\n", n, n)
}

type checkWriter struct {
	w       io.Writer
	written bool
}

func (w *checkWriter) Write(p []byte) (int, error) {
	w.written = true
	return w.w.Write(p)
}

func valuesFromString(s string) []quad.Value {
	if s == "" {
		return nil
	}
	arr := strings.Split(s, ",")
	out := make([]quad.Value, 0, len(arr))
	for _, s := range arr {
		out = append(out, quad.StringToValue(s))
	}
	return out
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
	values := shape.FilterQuads(
		valuesFromString(r.FormValue("sub")),
		valuesFromString(r.FormValue("pred")),
		valuesFromString(r.FormValue("obj")),
		valuesFromString(r.FormValue("label")),
	)
	it := values.BuildIterator(h.QuadStore).Iterate()
	qr := graph.NewResultReader(h.QuadStore, it)

	defer qr.Close()

	wr := writerFrom(w, r, hdrAcceptEncoding)
	defer wr.Close()

	cw := &checkWriter{w: wr}
	qwc := format.Writer(cw)
	defer qwc.Close()
	var qw quad.Writer = qwc
	if len(format.Mime) != 0 {
		w.Header().Set(hdrContentType, format.Mime[0])
	}
	if irif := r.FormValue("iri"); irif != "" {
		opts := quad.IRIOptions{
			Format: quad.IRIDefault,
		}
		switch irif {
		case "short":
			opts.Format = quad.IRIShort
		case "full":
			opts.Format = quad.IRIFull
		}
		qw = quad.IRIWriter(qw, opts)
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
		ID     string   `json:"id"`
		Read   bool     `json:"read,omitempty"`
		Write  bool     `json:"write,omitempty"`
		Nodes  bool     `json:"nodes,omitempty"`
		Ext    []string `json:"ext,omitempty"`
		Mime   []string `json:"mime,omitempty"`
		Binary bool     `json:"binary,omitempty"`
	}
	formats := quad.Formats()
	out := make([]Format, 0, len(formats))
	for _, f := range formats {
		out = append(out, Format{
			ID:  f.Name,
			Ext: f.Ext, Mime: f.Mime,
			Read: f.Reader != nil, Write: f.Writer != nil,
			Nodes:  f.UnmarshalValue != nil,
			Binary: f.Binary,
		})
	}
	w.Header().Set(hdrContentType, contentTypeJSON)
	json.NewEncoder(w).Encode(out)
}

func (api *APIv2) queryContext(r *http.Request) (ctx context.Context, cancel func()) {
	ctx = r.Context()
	if api.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, api.timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	return ctx, cancel
}

func defaultErrorFunc(w query.ResponseWriter, err error) {
	data, _ := json.Marshal(err.Error())
	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte(`{"error": `))
	w.Write(data)
	w.Write([]byte("}\n"))
}

func writeResults(w io.Writer, r interface{}) {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.Encode(map[string]interface{}{
		"result": r,
	})
}

const maxQuerySize = 1024 * 1024 // 1 MB
func readLimit(r io.Reader) ([]byte, error) {
	lr := io.LimitReader(r, maxQuerySize).(*io.LimitedReader)
	data, err := ioutil.ReadAll(lr)
	if err != nil && lr.N <= 0 {
		err = errors.New("request is too large")
	}
	return data, err
}

func (api *APIv2) ServeQuery(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := api.queryContext(r)
	defer cancel()
	vals := r.URL.Query()
	lang := vals.Get("lang")
	if lang == "" {
		jsonResponse(w, http.StatusBadRequest, "query language not specified")
		return
	}
	l := query.GetLanguage(lang)
	if l == nil {
		jsonResponse(w, http.StatusBadRequest, "unknown query language")
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
	h, err := api.handleForRequest(r)
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
		errFunc(w, errors.New("HTTP interface is not supported for this query language"))
		return
	}
	ses := l.Session(h.QuadStore)
	var qu string
	if r.Method == "GET" {
		qu = vals.Get("qu")
	} else {
		data, err := readLimit(r.Body)
		if err != nil {
			errFunc(w, err)
			return
		}
		qu = string(data)
	}
	if qu == "" {
		jsonResponse(w, http.StatusBadRequest, "query is empty")
		return
	}
	if clog.V(1) {
		clog.Infof("query: %s: %q", lang, qu)
	}

	opt := query.Options{
		Collation: query.JSON, // TODO: switch to JSON-LD by default when the time comes
		Limit:     api.limit,
	}
	if specs := ParseAccept(r.Header, hdrAccept); len(specs) != 0 {
		// TODO: sort by Q
		switch specs[0].Value {
		case contentTypeJSON:
			opt.Collation = query.JSON
		case contentTypeJSONLD:
			opt.Collation = query.JSONLD
		}
	}
	it, err := ses.Execute(ctx, qu, opt)
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
	if opt.Collation == query.JSONLD {
		w.Header().Set(hdrContentType, contentTypeJSONLD)
	} else {
		w.Header().Set(hdrContentType, contentTypeJSON)
	}
	writeResults(w, out)
}
