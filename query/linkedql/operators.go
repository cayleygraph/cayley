package linkedql

import (
	"regexp"

	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/cayley/query/shape"
	"github.com/cayleygraph/quad"
)

// Operator represents an operator used in a query inside a step (e.g. greater than).
type Operator interface {
	RegistryItem
	Apply(p *path.Path) (*path.Path, error)
}

var _ Operator = (*RegExp)(nil)

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

var _ Operator = (*Like)(nil)

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
