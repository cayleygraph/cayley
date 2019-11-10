package linkedql

import (
	"errors"
	"regexp"

	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/cayley/query/shape"
	"github.com/cayleygraph/quad"
)

// Operator represents an operator used in a query inside a step (e.g. greater than).
type Operator interface {
	RegistryItem
	Apply(p *path.Path) (*path.Path, error)
}

// LessThan corresponds to lt().
type LessThan struct {
	Value quad.Value `json:"value"`
}

// Type implements Operator.
func (s *LessThan) Type() quad.IRI {
	return prefix + "LessThan"
}

// Apply implements Operator.
func (s *LessThan) Apply(p *path.Path) (*path.Path, error) {
	return p.Filter(iterator.CompareLT, s.Value), nil
}

// LessThanEquals corresponds to lte().
type LessThanEquals struct {
	Value quad.Value `json:"value"`
}

// Type implements Operator.
func (s *LessThanEquals) Type() quad.IRI {
	return prefix + "LessThanEquals"
}

// Apply implements Operator.
func (s *LessThanEquals) Apply(p *path.Path) (*path.Path, error) {
	return p.Filter(iterator.CompareLTE, s.Value), nil
}

// GreaterThan corresponds to gt().
type GreaterThan struct {
	Value quad.Value `json:"value"`
}

// Apply implements Operator.
func (s *GreaterThan) Apply(p *path.Path) (*path.Path, error) {
	return p.Filter(iterator.CompareGT, s.Value), nil
}

// Type implements Operator.
func (s *GreaterThan) Type() quad.IRI {
	return prefix + "GreaterThan"
}

// GreaterThanEquals corresponds to gte().
type GreaterThanEquals struct {
	Value quad.Value `json:"value"`
}

// Type implements Operator.
func (s *GreaterThanEquals) Type() quad.IRI {
	return prefix + "GreaterThanEquals"
}

// Apply implements Operator.
func (s *GreaterThanEquals) Apply(p *path.Path) (*path.Path, error) {
	return p.Filter(iterator.CompareGTE, s.Value), nil
}

// RegExp corresponds to regex().
type RegExp struct {
	Pattern     string `json:"pattern"`
	IncludeIRIs bool   `json:"includeIRIs"`
}

// Type implements Operator.
func (s *RegExp) Type() quad.IRI {
	return prefix + "RegExp"
}

// Apply implements Operator.
func (s *RegExp) Apply(p *path.Path) (*path.Path, error) {
	pattern, err := regexp.Compile(string(s.Pattern))
	if err != nil {
		return nil, errors.New("Invalid RegExp")
	}
	if s.IncludeIRIs {
		return p.RegexWithRefs(pattern), nil
	}
	return p.RegexWithRefs(pattern), nil
}

// Like corresponds to like().
type Like struct {
	Pattern string `json:"pattern"`
}

// Type implements Operator.
func (s *Like) Type() quad.IRI {
	return prefix + "Like"
}

// Apply implements Operator.
func (s *Like) Apply(p *path.Path) (*path.Path, error) {
	return p.Filters(shape.Wildcard{Pattern: s.Pattern}), nil
}
