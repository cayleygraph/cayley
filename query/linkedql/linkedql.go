package linkedql

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/quad"
)

// Name is the name exposed to the query interface
const Name = "linkedql"

func init() {
	query.RegisterLanguage(query.Language{
		Name: Name,
		Session: func(qs graph.QuadStore) query.Session {
			return NewSession(qs)
		},
		HTTP: func(qs graph.QuadStore) query.HTTP {
			return NewSession(qs)
		},
	})
}

type Step interface {
	Type() quad.IRI
}
