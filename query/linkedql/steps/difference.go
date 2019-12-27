package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&Difference{})
}

var _ linkedql.IteratorStep = (*Difference)(nil)
var _ linkedql.PathStep = (*Difference)(nil)

// Difference corresponds to .difference().
type Difference struct {
	From  linkedql.PathStep   `json:"from"`
	Steps []linkedql.PathStep `json:"steps"`
}

// Description implements Step.
func (s *Difference) Description() string {
	return "resolves to all the values resolved by the from step different then the values resolved by the provided steps. Caution: it might be slow to execute."
}

// BuildIterator implements linkedql.IteratorStep.
func (s *Difference) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return linkedql.NewValueIteratorFromPathStep(s, qs, ns)
}

// BuildPath implements linkedql.PathStep.
func (s *Difference) BuildPath(qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	fromPath, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	path := fromPath
	for _, step := range s.Steps {
		p, err := step.BuildPath(qs, ns)
		if err != nil {
			return nil, err
		}
		path = path.Except(p)
	}
	return path, nil
}
