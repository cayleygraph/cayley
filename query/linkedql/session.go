package linkedql

import (
	"context"
	"errors"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
)

var _ query.HTTP = &Session{}

// Session represents a LinkedQL query processing
type Session struct {
	qs graph.QuadStore
}

// NewSession creates a new Session
func NewSession(qs graph.QuadStore) *Session {
	return &Session{
		qs: qs,
	}
}

// Execute for a given context, query and options return an iterator of results
func (s *Session) Execute(ctx context.Context, query string, opt query.Options) (query.Iterator, error) {
	step, err := UnmarshalStep([]byte(query))
	if err != nil {
		return nil, err
	}
	iterator := step.BuildIterator(s.qs)
	return iterator, nil
}

// ShapeOf returns for given query a Shape
func (s *Session) ShapeOf(query string) (interface{}, error) {
	return nil, errors.New("Not implemented")
}
