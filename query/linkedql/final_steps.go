package linkedql

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/quad"
)

func init() {
	Register(&Select{})
	Register(&SelectFirst{})
	Register(&Value{})
}

// Select corresponds to .select()
type Select struct {
	Tags []string  `json:"tags"`
	From ValueStep `json:"from"`
}

// Type implements Step
func (s *Select) Type() quad.IRI {
	return prefix + "Select"
}

// BuildIterator implements Step
func (s *Select) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	fromIt, err := s.From.BuildValueIterator(qs)
	if err != nil {
		return nil, err
	}
	return &TagsIterator{fromIt, s.Tags}, nil
}

// SelectFirst corresponds to .selectFirst()
type SelectFirst struct {
	Tags []string  `json:"tags"`
	From ValueStep `json:"from"`
}

// Type implements Step
func (s *SelectFirst) Type() quad.IRI {
	return prefix + "SelectFirst"
}

func singleValueIterator(it *ValueIterator) *ValueIterator {
	p := it.path.Limit(1)
	return NewValueIterator(p, it.namer)
}

// BuildIterator implements Step
func (s *SelectFirst) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	it, err := s.From.BuildValueIterator(qs)
	if err != nil {
		return nil, err
	}
	return &TagsIterator{singleValueIterator(it), s.Tags}, nil
}

// Value corresponds to .value()
type Value struct {
	From ValueStep `json:"from"`
}

// Type implements Step
func (s *Value) Type() quad.IRI {
	return prefix + "Value"
}

// BuildIterator implements Step
func (s *Value) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	it, err := s.From.BuildValueIterator(qs)
	if err != nil {
		return nil, err
	}
	return singleValueIterator(it), nil
}
