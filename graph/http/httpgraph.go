package httpgraph

import (
	"github.com/cayleygraph/cayley/graph"
	"net/http"
)

type QuadStore interface {
	graph.QuadStore
	ForRequest(r *http.Request) (graph.QuadStore, error)
}
