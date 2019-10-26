package linkedql

import (
	"context"
	"errors"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
)

var _ query.Session = &Session{}

// Session represents a LinkedQL query processing.
type Session struct {
	qs graph.QuadStore
}

// NewSession creates a new Session.
func NewSession(qs graph.QuadStore) *Session {
	return &Session{
		qs: qs,
	}
}

// Execute for a given context, query and options return an iterator of results.
func (s *Session) Execute(ctx context.Context, query string, opt query.Options) (query.Iterator, error) {
	item, err := Unmarshal([]byte(query))
	if err != nil {
		return nil, err
	}
	step, ok := item.(Step)
	if !ok {
		return nil, errors.New("Must execute a valid step")
	}
	return step.BuildIterator(s.qs)
}
