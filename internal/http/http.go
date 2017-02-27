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
	"flag"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"time"

	"github.com/codelingo/cayley/clog"
	"github.com/julienschmidt/httprouter"

	"github.com/codelingo/cayley/graph"
	"github.com/codelingo/cayley/internal/config"
	"github.com/codelingo/cayley/internal/db"
)

type ResponseHandler func(http.ResponseWriter, *http.Request, httprouter.Params) int

var assetsPath = flag.String("assets", "", "Explicit path to the HTTP assets.")
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
	if *assetsPath != "" {
		if hasAssets(*assetsPath) {
			return *assetsPath
		}
		clog.Fatalf("Cannot find assets at", *assetsPath, ".")
	}

	if hasAssets(".") {
		return "."
	}

	if hasAssets("..") {
		return ".."
	}

	gopathPath := os.ExpandEnv("$GOPATH/src/github.com/codelingo/cayley")
	if hasAssets(gopathPath) {
		return gopathPath
	}
	clog.Fatalf("Cannot find assets in any of the default search paths. Please run in the same directory, in a Go workspace, or set --assets .")
	panic("cannot reach")
}

func LogRequest(handler ResponseHandler) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		start := time.Now()
		addr := req.Header.Get("X-Real-IP")
		if addr == "" {
			addr = req.Header.Get("X-Forwarded-For")
			if addr == "" {
				addr = req.RemoteAddr
			}
		}
		clog.Infof("Started %s %s for %s", req.Method, req.URL.Path, addr)
		code := handler(w, req, params)
		clog.Infof("Completed %v %s %s in %v", code, http.StatusText(code), req.URL.Path, time.Since(start))

	}
}

func jsonResponse(w http.ResponseWriter, code int, err interface{}) int {
	w.Header().Set("Content-Type", contentTypeJSON)
	w.Write([]byte(`{"error": `))
	data, _ := json.Marshal(fmt.Sprint(err))
	w.Write(data)
	w.Write([]byte(`}`))
	return code
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
	if !api.config.RequiresHTTPRequestContext {
		return api.handle, nil
	}

	opts := make(graph.Options)
	opts["HTTPRequest"] = r

	qs, err := graph.NewQuadStoreForRequest(api.handle.QuadStore, opts)
	if err != nil {
		return nil, err
	}
	qw, err := db.OpenQuadWriter(qs, api.config)
	if err != nil {
		return nil, err
	}
	return &graph.Handle{QuadStore: qs, QuadWriter: qw}, nil
}

func (api *API) RWOnly(handler httprouter.Handle) httprouter.Handle {
	if api.config.ReadOnly {
		return func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
			jsonResponse(w, http.StatusForbidden, "Database is read-only.")
		}
	}
	return handler
}

func (api *API) APIv1(r *httprouter.Router) {
	r.POST("/api/v1/query/:query_lang", LogRequest(api.ServeV1Query))
	r.POST("/api/v1/shape/:query_lang", LogRequest(api.ServeV1Shape))
	r.POST("/api/v1/write", api.RWOnly(LogRequest(api.ServeV1Write)))
	r.POST("/api/v1/write/file/nquad", api.RWOnly(LogRequest(api.ServeV1WriteNQuad)))
	r.POST("/api/v1/delete", api.RWOnly(LogRequest(api.ServeV1Delete)))
}
func (api *API) APIv2(r *httprouter.Router) {
	r.POST("/api/v2/write", api.RWOnly(LogRequest(api.ServeV2Write)))
	r.POST("/api/v2/delete", api.RWOnly(LogRequest(api.ServeV2Delete)))
	r.POST("/api/v2/read", api.RWOnly(LogRequest(api.ServeV2Read)))
	r.GET("/api/v2/read", api.RWOnly(LogRequest(api.ServeV2Read)))
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
	api.APIv1(r)
	api.APIv2(r)

	//m.Use(martini.Static("static", martini.StaticOptions{Prefix: "/static", SkipLogging: true}))
	//r.Handler("GET", "/static", http.StripPrefix("/static", http.FileServer(http.Dir("static/"))))
	r.GET("/docs/:docpage", docs.ServeHTTP)
	r.GET("/ui/:ui_type", root.ServeHTTP)
	r.GET("/", root.ServeHTTP)
	http.Handle("/static/", http.StripPrefix("/static", http.FileServer(http.Dir(fmt.Sprint(assets, "/static/")))))
	http.Handle("/", r)
}

func Serve(handle *graph.Handle, cfg *config.Config) {
	SetupRoutes(handle, cfg)
	clog.Infof("Cayley now listening on %s:%s\n", cfg.ListenHost, cfg.ListenPort)
	fmt.Printf("Cayley now listening on %s:%s\n", cfg.ListenHost, cfg.ListenPort)
	err := http.ListenAndServe(fmt.Sprintf("%s:%s", cfg.ListenHost, cfg.ListenPort), nil)
	if err != nil {
		clog.Fatalf("ListenAndServe: %v", err)
	}
}
