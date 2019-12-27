package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&Entities{})
}

var _ linkedql.IteratorStep = (*Entities)(nil)
var _ linkedql.PathStep = (*Entities)(nil)

// Entities corresponds to g.Entities().
type Entities struct {
	Identifiers []linkedql.EntityIdentifier `json:"identifiers"`
}

// Description implements Step.
func (s *Entities) Description() string {
	return "resolves to all the existing objects in the graph. If provided with identifiers resolves to a sublist of all the existing identifiers in the graph."
}

// BuildIterator implements linkedql.IteratorStep.
func (s *Entities) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *Entities) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	var values []quad.Value
	for _, identifier := range s.Identifiers {
		value, err := identifier.BuildIdentifier(ns)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	// TODO(iddan): Construct a path that only match entities
	return path.StartPath(qs, values...), nil
}
