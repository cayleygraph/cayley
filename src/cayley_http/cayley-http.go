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

package cayley_http

import (
	cfg "cayley_config"
	"fmt"
	"github.com/barakmich/glog"
	"github.com/julienschmidt/httprouter"
	"graph"
	"html/template"
	"net/http"
	"time"
)

type ResponseHandler func(http.ResponseWriter, *http.Request, httprouter.Params) int

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
	config *cfg.CayleyConfig
	ts     graph.TripleStore
}

func (api *Api) ApiV1(router *httprouter.Router) {
	router.POST("/api/v1/query/:query_lang", LogRequest(api.ServeV1Query))
	router.POST("/api/v1/shape/:query_lang", LogRequest(api.ServeV1Shape))
	router.POST("/api/v1/write", LogRequest(api.ServeV1Write))
	router.POST("/api/v1/write/file/nquad", LogRequest(api.ServeV1WriteNQuad))
	//TODO(barakmich): /write/text/nquad, which reads from request.body instead of HTML5 file form?
	router.POST("/api/v1/delete", LogRequest(api.ServeV1Delete))
}

func SetupRoutes(ts graph.TripleStore, config *cfg.CayleyConfig) {
	router := httprouter.New()
	templates := template.Must(template.ParseGlob("templates/*.tmpl"))

	templates.ParseGlob("templates/*.html")

	root := &TemplateRequestHandler{
		templates: templates,
	}
	docs := &DocRequestHandler{}
	api := &Api{
		config: config,
		ts:     ts,
	}

	api.ApiV1(router)

	router.GET("/docs/:docpage", docs.ServeHTTP)
	router.GET("/ui/:ui_type", root.ServeHTTP)
	router.GET("/", root.ServeHTTP)
	http.Handle("/static/", http.StripPrefix("/static", http.FileServer(http.Dir("static/"))))
	http.Handle("/", router)
}

func CayleyHTTP(ts graph.TripleStore, config *cfg.CayleyConfig) {
	SetupRoutes(ts, config)
	glog.Infof("Cayley now listening on %s:%s\n", config.ListenHost, config.ListenPort)
	fmt.Printf("Cayley now listening on %s:%s\n", config.ListenHost, config.ListenPort)

	err := http.ListenAndServe(fmt.Sprintf("%s:%s", config.ListenHost, config.ListenPort), nil)

	if err != nil {
		glog.Fatal("ListenAndServe: ", err)
	}
}
