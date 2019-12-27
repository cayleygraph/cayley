package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&Select{})
	linkedql.Register(&SelectFirst{})
	linkedql.Register(&Value{})
	linkedql.Register(&Documents{})
}

var _ linkedql.IteratorStep = (*Select)(nil)

// Select corresponds to .select().
type Select struct {
	Tags []string          `json:"tags"`
	From linkedql.PathStep `json:"from"`
}

// Description implements Step.
func (s *Select) Description() string {
	return "Select returns flat records of tags matched in the query"
}

// BuildIterator implements IteratorStep
func (s *Select) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	valueIt, err := linkedql.NewValueIteratorFromPathStep(s.From, qs, ns)
	if err != nil {
		return nil, err
	}
	return &linkedql.TagsIterator{ValueIt: valueIt, Selected: s.Tags}, nil
}

var _ linkedql.IteratorStep = (*SelectFirst)(nil)

// SelectFirst corresponds to .selectFirst().
type SelectFirst struct {
	Tags []string          `json:"tags"`
	From linkedql.PathStep `json:"from"`
}

// Description implements Step.
func (s *SelectFirst) Description() string {
	return "Like Select but only returns the first result"
}

func singleValueIteratorFromPathStep(step linkedql.PathStep, qs graph.QuadStore, ns *voc.Namespaces) (*linkedql.ValueIterator, error) {
	p, err := step.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	return linkedql.NewValueIterator(p.Limit(1), qs), nil
}

// BuildIterator implements IteratorStep
func (s *SelectFirst) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	it, err := singleValueIteratorFromPathStep(s.From, qs, ns)
	if err != nil {
		return nil, err
	}
	return &linkedql.TagsIterator{it, s.Tags}, nil
}

var _ linkedql.IteratorStep = (*Value)(nil)

// Value corresponds to .value().
type Value struct {
	From linkedql.PathStep `json:"from"`
}

// Description implements Step.
func (s *Value) Description() string {
	return "Value returns a single value matched in the query"
}

// BuildIterator implements IteratorStep
func (s *Value) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return singleValueIteratorFromPathStep(s.From, qs, ns)
}

var _ linkedql.IteratorStep = (*Documents)(nil)

// Documents corresponds to .documents().
type Documents struct {
	From linkedql.PathStep `json:"from"`
}

// Description implements Step.
func (s *Documents) Description() string {
	return "Documents return documents of the tags matched in the query associated with their entity"
}

// BuildIterator implements IteratorStep
func (s *Documents) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	p, err := s.From.BuildPath(qs, ns)
	if err != nil {
		return nil, err
	}
	it, err := linkedql.NewValueIterator(p, qs), nil
	if err != nil {
		return nil, err
	}
	return linkedql.NewDocumentIterator(it), nil
}
