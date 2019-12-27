package steps

import (
	"regexp"

	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/cayley/query/shape"
)

// RegExp corresponds to regex().
type RegExp struct {
	Pattern     string `json:"pattern"`
	IncludeIRIs bool   `json:"includeIRIs,omitempty"`
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

var _ linkedql.Operator = (*Like)(nil)

// Like corresponds to like().
type Like struct {
	Pattern string `json:"pattern"`
}

// Description implements Operator.
func (s *Like) Description() string {
	return "Like filters out values that do not match given pattern."
}

// Apply implements Operator.
func (s *Like) Apply(p *path.Path) (*path.Path, error) {
	return p.Filters(shape.Wildcard{Pattern: s.Pattern}), nil
}
