package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&Select{})
}

var _ linkedql.IteratorStep = (*Select)(nil)

// Select corresponds to .select().
type Select struct {
	Names *linkedql.PropertyPath `json:"names"`
	From  linkedql.PathStep      `json:"from"`
}

// Description implements Step.
func (s *Select) Description() string {
	return "Select returns flat records of tags matched in the query"
}

// BuildIterator implements IteratorStep
func (s *Select) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	properties, err := resolveNames(s.Names)
	if err != nil {
		return nil, err
	}
	path, err := s.From.BuildPath(qs, ns)
	it := linkedql.NewQuadIterator(qs, path, properties)
	return it, nil
}
