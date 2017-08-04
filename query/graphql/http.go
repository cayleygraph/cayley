package graphql

import (
	"context"
	"encoding/json"
	"io"

	"github.com/dennwc/graphql/gqlerrors"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
)

type httpResult struct {
	Data   interface{}                `json:"data"`
	Errors []gqlerrors.FormattedError `json:"errors,omitempty"`
}

func httpError(w query.ResponseWriter, err error) {
	json.NewEncoder(w).Encode(httpResult{
		Errors: []gqlerrors.FormattedError{{
			Message: err.Error(),
		}},
	})
}

func httpQuery(ctx context.Context, qs graph.QuadStore, w query.ResponseWriter, r io.Reader) {
	q, err := Parse(r)
	if err != nil {
		httpError(w, err)
		return
	}
	m, err := q.Execute(ctx, qs)
	if err != nil {
		httpError(w, err)
		return
	}
	json.NewEncoder(w).Encode(httpResult{Data: m})
}
