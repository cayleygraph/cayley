package linkedql

import (
	"context"
	"errors"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/quad/voc"
)

const (
	// Name is the name exposed to the query interface.
	Name = "linkedql"
	// Namespace is an RDF namespace used for LinkedQL classes.
	Namespace = "http://cayley.io/linkedql#"
	// Prefix is an RDF namespace prefix used for LinkedQL classes.
	Prefix = "linkedql:"
)

func init() {
	// IRI namespace support
	voc.Register(voc.Namespace{Full: Namespace, Prefix: Prefix})
	// register the language
	query.RegisterLanguage(query.Language{
		Name: Name,
		Session: func(qs graph.QuadStore) query.Session {
			return NewSession(qs)
		},
	})
}

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
	step, ok := item.(IteratorStep)
	if !ok {
		return nil, errors.New("must execute a valid step")
	}
	ns := voc.Namespaces{}
	return step.BuildIterator(s.qs, &ns)
}
