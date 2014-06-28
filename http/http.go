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
	"flag"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"time"

	"github.com/barakmich/glog"
	"github.com/julienschmidt/httprouter"

	"github.com/google/cayley/config"
	"github.com/google/cayley/graph"
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
		} else {
			glog.Fatalln("Cannot find assets at", *assetsPath, ".")
		}
	}

	if hasAssets(".") {
		return "."
	}

	gopathPath := os.ExpandEnv("$GOPATH/src/github.com/google/cayley")
	if hasAssets(gopathPath) {
		return gopathPath
	}
	glog.Fatalln("Cannot find assets in any of the default search paths. Please run in the same directory, in a Go workspace, or set --assets .")
	return ""
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
		glog.Infof("Started %s %s for %s", req.Method, req.URL.Path, addr)
		code := handler(w, req, params)
		glog.Infof("Completed %v %s %s in %v", code, http.StatusText(code), req.URL.Path, time.Since(start))

	}
}

func FormatJson400(w http.ResponseWriter, err interface{}) int {
	return FormatJsonError(w, 400, err)
}

func FormatJsonError(w http.ResponseWriter, code int, err interface{}) int {
	http.Error(w, fmt.Sprintf("{\"error\" : \"%s\"}", err), code)
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

type Api struct {
	config *config.CayleyConfig
	ts     graph.TripleStore
}

func (api *Api) ApiV1(r *httprouter.Router) {
	r.POST("/api/v1/query/:query_lang", LogRequest(api.ServeV1Query))
	r.POST("/api/v1/shape/:query_lang", LogRequest(api.ServeV1Shape))
	r.POST("/api/v1/write", LogRequest(api.ServeV1Write))
	r.POST("/api/v1/write/file/nquad", LogRequest(api.ServeV1WriteNQuad))
	//TODO(barakmich): /write/text/nquad, which reads from request.body instead of HTML5 file form?
	r.POST("/api/v1/delete", LogRequest(api.ServeV1Delete))
}

func SetupRoutes(ts graph.TripleStore, cfg *config.CayleyConfig) {
	r := httprouter.New()
	assets := findAssetsPath()
	if glog.V(2) {
		glog.V(2).Infoln("Found assets at", assets)
	}
	var templates = template.Must(template.ParseGlob(fmt.Sprint(assets, "/templates/*.tmpl")))
	templates.ParseGlob(fmt.Sprint(assets, "/templates/*.html"))
	root := &TemplateRequestHandler{templates: templates}
	docs := &DocRequestHandler{assets: assets}
	api := &Api{config: cfg, ts: ts}
	api.ApiV1(r)

	//m.Use(martini.Static("static", martini.StaticOptions{Prefix: "/static", SkipLogging: true}))
	//r.Handler("GET", "/static", http.StripPrefix("/static", http.FileServer(http.Dir("static/"))))
	r.GET("/docs/:docpage", docs.ServeHTTP)
	r.GET("/ui/:ui_type", root.ServeHTTP)
	r.GET("/", root.ServeHTTP)
	http.Handle("/static/", http.StripPrefix("/static", http.FileServer(http.Dir(fmt.Sprint(assets, "/static/")))))
	http.Handle("/", r)
}

func CayleyHTTP(ts graph.TripleStore, cfg *config.CayleyConfig) {
	SetupRoutes(ts, cfg)
	glog.Infof("Cayley now listening on %s:%s\n", cfg.ListenHost, cfg.ListenPort)
	fmt.Printf("Cayley now listening on %s:%s\n", cfg.ListenHost, cfg.ListenPort)
	err := http.ListenAndServe(fmt.Sprintf("%s:%s", cfg.ListenHost, cfg.ListenPort), nil)
	if err != nil {
		glog.Fatal("ListenAndServe: ", err)
	}
}
