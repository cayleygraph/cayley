package http

import (
	"net/http"

	"github.com/cayleygraph/cayley/graph"
	cayleyhttp "github.com/cayleygraph/cayley/server/http"
	"github.com/julienschmidt/httprouter"
)

type API struct {
	config *Config
	handle *graph.Handle
}

func (api *API) GetHandleForRequest(r *http.Request) (*graph.Handle, error) {
	return cayleyhttp.HandleForRequest(api.handle, "single", nil, r)
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
	r.POST("/query/:query_lang", api.ServeV1Query)
	r.POST("/shape/:query_lang", api.ServeV1Shape)
	r.POST("/write", api.RWOnly(api.ServeV1Write))
	r.POST("/write/file/nquad", api.RWOnly(api.ServeV1WriteNQuad))
	r.POST("/delete", api.RWOnly(api.ServeV1Delete))
}
