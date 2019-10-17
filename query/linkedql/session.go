package linkedql

import (
	"context"
	"errors"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
)

const noLimit = -1

var _ query.HTTP = &Session{}

// Session represents a LinkedQL query processing
type Session struct {
	qs    graph.QuadStore
	limit int
}

// NewSession creates a new Session
func NewSession(qs graph.QuadStore) *Session {
	return &Session{
		qs:    qs,
		limit: noLimit,
	}
}

// Execute for a given context, query and options return an iterator of results
func (session *Session) Execute(ctx context.Context, query string, opt query.Options) (query.Iterator, error) {
	return nil, errors.New("Not implemented")
}

// ShapeOf returns for given query a Shape
func (session *Session) ShapeOf(query string) (interface{}, error) {
	return nil, errors.New("Not implemented")
}
