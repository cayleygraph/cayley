package cayleyhttp

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/http"
)

func jsonResponse(w http.ResponseWriter, code int, err interface{}) {
	w.Header().Set("Content-Type", contentTypeJSON)
	w.WriteHeader(code)
	w.Write([]byte(`{"error": `))
	var s string
	switch err := err.(type) {
	case string:
		s = err
	case error:
		s = err.Error()
	default:
		s = fmt.Sprint(err)
	}
	data, _ := json.Marshal(s)
	w.Write(data)
	w.Write([]byte(`}`))
}

func HandleForRequest(h *graph.Handle, wtyp string, wopt graph.Options, r *http.Request) (*graph.Handle, error) {
	g, ok := h.QuadStore.(httpgraph.QuadStore)
	if !ok {
		return h, nil
	}
	qs, err := g.ForRequest(r)
	if err != nil {
		return nil, err
	}
	qw, err := graph.NewQuadWriter(wtyp, qs, wopt)
	if err != nil {
		qs.Close()
		return nil, err
	}
	return &graph.Handle{QuadStore: qs, QuadWriter: qw}, nil
}
