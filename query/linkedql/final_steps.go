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

// Select corresponds to .select().
type Select struct {
	Tags []string `json:"tags"`
	From PathStep `json:"from"`
}

// Type implements Step.
func (s *Select) Type() quad.IRI {
	return prefix + "Select"
}

// BuildIterator implements Step.
func (s *Select) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	valueIt, err := NewValueIteratorFromPathStep(s.From, qs)
	if err != nil {
		return nil, err
	}
	return &TagsIterator{valueIt: valueIt, selected: s.Tags}, nil
}

// SelectFirst corresponds to .selectFirst().
type SelectFirst struct {
	Tags []string `json:"tags"`
	From PathStep `json:"from"`
}

// Type implements Step.
func (s *SelectFirst) Type() quad.IRI {
	return prefix + "SelectFirst"
}

func singleValueIteratorFromPathStep(step PathStep, qs graph.QuadStore) (*ValueIterator, error) {
	p, err := step.BuildPath(qs)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(p.Limit(1), qs), nil
}

// BuildIterator implements Step.
func (s *SelectFirst) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	it, err := singleValueIteratorFromPathStep(s.From, qs)
	if err != nil {
		return nil, err
	}
	return &TagsIterator{it, s.Tags}, nil
}

// Value corresponds to .value().
type Value struct {
	From PathStep `json:"from"`
}

// Type implements Step.
func (s *Value) Type() quad.IRI {
	return prefix + "Value"
}

// BuildIterator implements Step.
func (s *Value) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	return singleValueIteratorFromPathStep(s.From, qs)
}

// Documents corresponds to .documents().
type Documents struct {
	From DocumentStep `json:"from"`
}

// Type implements Step.
func (s *Documents) Type() quad.IRI {
	return prefix + "Documents"
}

// BuildIterator implements Step.
func (s *Documents) BuildIterator(qs graph.QuadStore) (query.Iterator, error) {
	it, err := s.From.BuildDocumentIterator(qs)
	if err != nil {
		return nil, err
	}
	return it, nil
}
