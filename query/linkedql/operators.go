package linkedql

import (
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
	return Prefix + "LessThan"
}

// Description implements Operator.
func (s *LessThan) Description() string {
	return "Less than filters out values that are not less than given value"
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
	return Prefix + "LessThanEquals"
}

// Description implements Operator.
func (s *LessThanEquals) Description() string {
	return "Less than equals filters out values that are not less than or equal given value"
}

// Apply implements Operator.
func (s *LessThanEquals) Apply(p *path.Path) (*path.Path, error) {
	return p.Filter(iterator.CompareLTE, s.Value), nil
}

// GreaterThan corresponds to gt().
type GreaterThan struct {
	Value quad.Value `json:"value"`
}

// Type implements Operator.
func (s *GreaterThan) Type() quad.IRI {
	return Prefix + "GreaterThan"
}

// Description implements Operator.
func (s *GreaterThan) Description() string {
	return "Greater than equals filters out values that are not greater than given value"
}

// Apply implements Operator.
func (s *GreaterThan) Apply(p *path.Path) (*path.Path, error) {
	return p.Filter(iterator.CompareGT, s.Value), nil
}

// GreaterThanEquals corresponds to gte().
type GreaterThanEquals struct {
	Value quad.Value `json:"value"`
}

// Type implements Operator.
func (s *GreaterThanEquals) Type() quad.IRI {
	return Prefix + "GreaterThanEquals"
}

// Description implements Operator.
func (s *GreaterThanEquals) Description() string {
	return "Greater than equals filters out values that are not greater than or equal given value"
}

// Apply implements Operator.
func (s *GreaterThanEquals) Apply(p *path.Path) (*path.Path, error) {
	return p.Filter(iterator.CompareGTE, s.Value), nil
}

// RegExp corresponds to regex().
type RegExp struct {
	Pattern     string `json:"pattern"`
	IncludeIRIs bool   `json:"includeIRIs,omitempty"`
}

// Type implements Operator.
func (s *RegExp) Type() quad.IRI {
	return Prefix + "RegExp"
}

// Description implements Operator.
func (s *RegExp) Description() string {
	return "RegExp filters out values that do not match given pattern. If includeIRIs is set to true it matches IRIs in addition to literals."
}

// Apply implements Operator.
func (s *RegExp) Apply(p *path.Path) (*path.Path, error) {
	pattern, err := regexp.Compile(s.Pattern)
	if err != nil {
		return nil, err
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
	return Prefix + "Like"
}

// Description implements Operator.
func (s *Like) Description() string {
	return "Like filters out values that do not match given pattern."
}

// Apply implements Operator.
func (s *Like) Apply(p *path.Path) (*path.Path, error) {
	return p.Filters(shape.Wildcard{Pattern: s.Pattern}), nil
}
