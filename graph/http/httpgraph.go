package httpgraph

import (
	"net/http"

	"github.com/codelingo/cayley/graph"
)

type QuadStore interface {
	graph.QuadStore
	ForRequest(r *http.Request) (graph.QuadStore, error)
}
