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
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/julienschmidt/httprouter"

	"github.com/codelingo/cayley/clog"
	"github.com/codelingo/cayley/graph"
	"github.com/codelingo/cayley/internal"
	"github.com/codelingo/cayley/quad"
	"github.com/codelingo/cayley/quad/nquads"
)

func ParseJSONToQuadList(jsonBody []byte) (out []quad.Quad, _ error) {
	var quads []struct {
		Subject   string `json:"subject"`
		Predicate string `json:"predicate"`
		Object    string `json:"object"`
		Label     string `json:"label"`
	}
	err := json.Unmarshal(jsonBody, &quads)
	if err != nil {
		return nil, err
	}
	out = make([]quad.Quad, 0, len(quads))
	for i, jq := range quads {
		q := quad.Quad{
			Subject:   quad.StringToValue(jq.Subject),
			Predicate: quad.StringToValue(jq.Predicate),
			Object:    quad.StringToValue(jq.Object),
			Label:     quad.StringToValue(jq.Label),
		}
		if !q.IsValid() {
			return nil, fmt.Errorf("invalid quad at index %d. %s", i, q)
		}
		out = append(out, q)
	}
	return out, nil
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
	h, err := api.GetHandleForRequest(r)
	if err != nil {
		return jsonResponse(w, 400, err)
	}
	if err = h.QuadWriter.AddQuadSet(quads); err != nil {
		return jsonResponse(w, 400, err)
	}
	fmt.Fprintf(w, "{\"result\": \"Successfully wrote %d quads.\"}", len(quads))
	return 200
}

func (api *API) ServeV1WriteNQuad(w http.ResponseWriter, r *http.Request, params httprouter.Params) int {
	if api.config.ReadOnly {
		return jsonResponse(w, 400, "Database is read-only.")
	}

	formFile, _, err := r.FormFile("NQuadFile")
	if err != nil {
		clog.Errorf("%v", err)
		return jsonResponse(w, 500, "Couldn't read file: "+err.Error())
	}
	defer formFile.Close()

	blockSize, blockErr := strconv.Atoi(r.URL.Query().Get("block_size"))
	if blockErr != nil {
		blockSize = api.config.LoadSize
	}

	quadReader, err := internal.Decompressor(formFile)
	// TODO(kortschak) Make this configurable from the web UI.
	dec := nquads.NewReader(quadReader, false)

	h, err := api.GetHandleForRequest(r)
	if err != nil {
		return jsonResponse(w, 400, err)
	}
	n, err := quad.CopyBatch(graph.NewWriter(h), dec, blockSize)
	if err != nil {
		return jsonResponse(w, 400, err)
	}

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
	h, err := api.GetHandleForRequest(r)
	if err != nil {
		return jsonResponse(w, 400, err)
	}
	for _, q := range quads {
		err = h.QuadWriter.RemoveQuad(q)
		if err != nil && !graph.IsQuadNotExist(err) {
			return jsonResponse(w, 400, err)
		}
	}
	fmt.Fprintf(w, "{\"result\": \"Successfully deleted %d quads.\"}", len(quads))
	return 200
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

func (api *API) ServeV2Write(w http.ResponseWriter, r *http.Request, _ httprouter.Params) int {
	defer r.Body.Close()
	format := getFormat(r, "", hdrContentType)
	if format == nil || format.Reader == nil {
		return jsonResponse(w, http.StatusBadRequest, fmt.Errorf("format is not supported for reading data"))
	}
	rd, err := readerFrom(r, hdrContentEncoding)
	if err != nil {
		return jsonResponse(w, http.StatusBadRequest, err)
	}
	defer rd.Close()
	qr := format.Reader(rd)
	defer qr.Close()
	h, err := api.GetHandleForRequest(r)
	if err != nil {
		return jsonResponse(w, http.StatusBadRequest, err)
	}
	qw := graph.NewWriter(h.QuadWriter)
	defer qw.Close()
	n, err := quad.CopyBatch(qw, qr, api.config.LoadSize)
	if err != nil {
		return jsonResponse(w, http.StatusInternalServerError, err)
	}
	w.Header().Set(hdrContentType, contentTypeJSON)
	fmt.Fprintf(w, `{"result": "Successfully wrote %d quads.", "count": %d}`+"\n", n, n)
	return 200
}

func (api *API) ServeV2Delete(w http.ResponseWriter, r *http.Request, _ httprouter.Params) int {
	defer r.Body.Close()
	format := getFormat(r, "", hdrContentType)
	if format == nil || format.Reader == nil {
		return jsonResponse(w, http.StatusBadRequest, fmt.Errorf("format is not supported for reading data"))
	}
	rd, err := readerFrom(r, hdrContentEncoding)
	if err != nil {
		return jsonResponse(w, http.StatusBadRequest, err)
	}
	defer rd.Close()
	qr := format.Reader(r.Body)
	defer qr.Close()
	h, err := api.GetHandleForRequest(r)
	if err != nil {
		return jsonResponse(w, http.StatusBadRequest, err)
	}
	qw := graph.NewRemover(h.QuadWriter)
	defer qw.Close()
	n, err := quad.CopyBatch(qw, qr, api.config.LoadSize)
	if err != nil {
		return jsonResponse(w, http.StatusInternalServerError, err)
	}
	w.Header().Set(hdrContentType, contentTypeJSON)
	fmt.Fprintf(w, `{"result": "Successfully deleted %d quads.", "count": %d}`+"\n", n, n)
	return 200
}

type checkWriter struct {
	w       io.Writer
	written bool
}

func (w *checkWriter) Write(p []byte) (int, error) {
	w.written = true
	return w.w.Write(p)
}

func (api *API) ServeV2Read(w http.ResponseWriter, r *http.Request, _ httprouter.Params) int {
	format := getFormat(r, "format", hdrAccept)
	if format == nil || format.Writer == nil {
		return jsonResponse(w, http.StatusBadRequest, fmt.Errorf("format is not supported for reading data"))
	}
	h, err := api.GetHandleForRequest(r)
	if err != nil {
		return jsonResponse(w, http.StatusBadRequest, err)
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
		_, err = quad.CopyBatch(bw, qr, api.config.LoadSize)
	} else {
		_, err = quad.Copy(qw, qr)
	}
	if err != nil && !cw.written {
		return jsonResponse(w, http.StatusInternalServerError, err)
	} else if err != nil {
		// can do nothing here, since first byte (and header) was written
		// TODO: check if client just gone away
		clog.Errorf("read quads error: %v", err)
		return 500
	}
	return 200
}
