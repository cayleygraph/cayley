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
	"net/http"
	"time"

	"github.com/gobuffalo/packr/v2"
	"github.com/julienschmidt/httprouter"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/internal/gephi"
	cayleyhttp "github.com/cayleygraph/cayley/server/http"
)

var ui = packr.New("UI", "../../ui")

func jsonResponse(w http.ResponseWriter, code int, err interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write([]byte(`{"error": `))
	data, _ := json.Marshal(fmt.Sprint(err))
	w.Write(data)
	w.Write([]byte(`}`))
}

// Config holds the HTTP server configuration
type Config struct {
	ReadOnly bool
	Timeout  time.Duration
	Batch    int
}

func SetupRoutes(handle *graph.Handle, cfg *Config) error {
	r := httprouter.New()

	// Health check
	r.HandlerFunc("GET", "/health", HandleHealth)

	// Handle CORS preflight request
	r.HandlerFunc("OPTIONS", "/*path", HandlePreflight)

	// Register API V1
	api := &API{config: cfg, handle: handle}
	api.APIv1(r)

	// Register Gephi API
	gs := &gephi.GraphStreamHandler{QS: handle.QuadStore}
	r.GET("/gephi/gs", gs.ServeHTTP)

	// Register API V2
	api2 := cayleyhttp.NewBoundAPIv2(handle, r)
	api2.SetReadOnly(cfg.ReadOnly)
	api2.SetBatchSize(cfg.Batch)
	api2.SetQueryTimeout(cfg.Timeout)

	// For non API requests serve the UI
	r.NotFound = http.FileServer(ui)

	http.Handle("/", CORS(LogRequest(r)))

	return nil
}
