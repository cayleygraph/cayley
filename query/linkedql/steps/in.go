package steps

import (
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/cayley/query/linkedql"
)

func init() {
	linkedql.Register(&In{})
}

var _ linkedql.IteratorStep = (*In)(nil)
var _ linkedql.PathStep = (*In)(nil)

// In is an alias for ViewReverse.
type In struct {
	VisitReverse
}

// Type implements Step.
func (s *In) Type() quad.IRI {
	return linkedql.Prefix + "In"
}

// Description implements Step.
func (s *In) Description() string {
	return "aliases for ViewReverse"
}
