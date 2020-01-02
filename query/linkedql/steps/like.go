package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/cayley/query/shape"
	"github.com/cayleygraph/quad/voc"
)

var _ linkedql.IteratorStep = (*Like)(nil)
var _ linkedql.PathStep = (*Like)(nil)

// Like corresponds to like().
type Like struct {
	From    linkedql.PathStep `json:"from"`
	Pattern string            `json:"pattern"`
}

// Description implements Operator.
func (s *Like) Description() string {
	return "Like filters out values that do not match given pattern."
}

// BuildIterator implements linkedql.IteratorStep.
func (s *Like) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements PathStep.
func (s *Like) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return fromPath.Filters(shape.Wildcard{Pattern: s.Pattern}), nil
}
