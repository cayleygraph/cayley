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
	"html/template"
	"net/http"
	"os"
	"time"

	"github.com/cayleygraph/cayley/clog"
	"github.com/julienschmidt/httprouter"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/internal/config"
	"github.com/cayleygraph/cayley/internal/gephi"
	"github.com/cayleygraph/cayley/server/http"
)

var AssetsPath string
var assetsDirs = []string{"templates", "static", "docs"}

func hasAssets(path string) bool {
	for _, dir := range assetsDirs {
		if _, err := os.Stat(fmt.Sprint(path, "/", dir)); os.IsNotExist(err) {
			return false
		}
	}
	return true
}

func findAssetsPath() string {
	if AssetsPath != "" {
		if hasAssets(AssetsPath) {
			return AssetsPath
		}
		clog.Fatalf("Cannot find assets at", AssetsPath, ".")
	}

	if hasAssets(".") {
		return "."
	}

	if hasAssets("..") {
		return ".."
	}

	gopathPath := os.ExpandEnv("$GOPATH/src/github.com/cayleygraph/cayley")
	if hasAssets(gopathPath) {
		return gopathPath
	}
	clog.Fatalf("Cannot find assets in any of the default search paths. Please run in the same directory, in a Go workspace, or set --assets .")
	panic("cannot reach")
}

type statusWriter struct {
	http.ResponseWriter
	code *int
}

func (w *statusWriter) WriteHeader(code int) {
	*(w.code) = code
}

func LogRequest(handler httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		start := time.Now()
		addr := req.Header.Get("X-Real-IP")
		if addr == "" {
			addr = req.Header.Get("X-Forwarded-For")
			if addr == "" {
				addr = req.RemoteAddr
			}
		}
		code := 200
		rw := &statusWriter{ResponseWriter: w, code: &code}
		clog.Infof("Started %s %s for %s", req.Method, req.URL.Path, addr)
		handler(rw, req, params)
		clog.Infof("Completed %v %s %s in %v", code, http.StatusText(code), req.URL.Path, time.Since(start))

	}
}

func jsonResponse(w http.ResponseWriter, code int, err interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write([]byte(`{"error": `))
	data, _ := json.Marshal(fmt.Sprint(err))
	w.Write(data)
	w.Write([]byte(`}`))
}

type TemplateRequestHandler struct {
	templates *template.Template
}

func (h *TemplateRequestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	uiType := params.ByName("ui_type")
	if r.URL.Path == "/" {
		uiType = "query"
	}
	err := h.templates.ExecuteTemplate(w, uiType+".html", h)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type API struct {
	config *config.Config
	handle *graph.Handle
}

func (api *API) GetHandleForRequest(r *http.Request) (*graph.Handle, error) {
	return cayleyhttp.HandleForRequest(api.handle, api.config.ReplicationType, api.config.ReplicationOptions, r)
}

func (api *API) RWOnly(handler httprouter.Handle) httprouter.Handle {
	if api.config.ReadOnly {
		return func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
			jsonResponse(w, http.StatusForbidden, "Database is read-only.")
		}
	}
	return handler
}

func CORSFunc(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
	if origin := req.Header.Get("Origin"); origin != "" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers",
			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
	}
}

func CORS(h httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		CORSFunc(w, req, params)
		h(w, req, params)
	}
}

func (api *API) APIv1(r *httprouter.Router) {
	r.POST("/api/v1/query/:query_lang", CORS(LogRequest(api.ServeV1Query)))
	r.POST("/api/v1/shape/:query_lang", CORS(LogRequest(api.ServeV1Shape)))
	r.POST("/api/v1/write", CORS(api.RWOnly(LogRequest(api.ServeV1Write))))
	r.POST("/api/v1/write/file/nquad", CORS(api.RWOnly(LogRequest(api.ServeV1WriteNQuad))))
	r.POST("/api/v1/delete", CORS(api.RWOnly(LogRequest(api.ServeV1Delete))))
}

func SetupRoutes(handle *graph.Handle, cfg *config.Config) {
	r := httprouter.New()
	assets := findAssetsPath()
	if clog.V(2) {
		clog.Infof("Found assets at %v", assets)
	}
	var templates = template.Must(template.ParseGlob(fmt.Sprint(assets, "/templates/*.tmpl")))
	templates.ParseGlob(fmt.Sprint(assets, "/templates/*.html"))
	root := &TemplateRequestHandler{templates: templates}
	docs := &DocRequestHandler{assets: assets}
	api := &API{config: cfg, handle: handle}
	r.OPTIONS("/*path", CORSFunc)
	api.APIv1(r)

	api2 := cayleyhttp.NewAPIv2Writer(handle, cfg.ReplicationType, cfg.ReplicationOptions)
	api2.SetReadOnly(cfg.ReadOnly)
	api2.SetBatchSize(cfg.LoadSize)
	api2.SetQueryTimeout(cfg.Timeout)
	api2.RegisterOn(r, CORS, LogRequest)

	gs := &gephi.GraphStreamHandler{QS: handle.QuadStore}
	const gephiPath = "/gephi/gs"
	r.GET(gephiPath, gs.ServeHTTP)
	fmt.Printf("Serving Gephi GraphStream at http://localhost:%s%s\n", cfg.ListenPort, gephiPath)

	//m.Use(martini.Static("static", martini.StaticOptions{Prefix: "/static", SkipLogging: true}))
	//r.Handler("GET", "/static", http.StripPrefix("/static", http.FileServer(http.Dir("static/"))))
	r.GET("/docs/:docpage", docs.ServeHTTP)
	r.GET("/ui/:ui_type", root.ServeHTTP)
	r.GET("/", root.ServeHTTP)
	http.Handle("/static/", http.StripPrefix("/static", http.FileServer(http.Dir(fmt.Sprint(assets, "/static/")))))
	http.Handle("/", r)
}
