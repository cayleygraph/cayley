package linkedql

import (
	"github.com/cayleygraph/quad"
)

// Operator represents an operator used in a query inside a step (e.g. greater than).
type Operator interface {
	RegistryItem
}

// LessThan corresponds to lt()
type LessThan struct {
	Value quad.Value `json:"value"`
}

// Type implements Step
func (s *LessThan) Type() quad.IRI {
	return prefix + "LessThan"
}

// LessThanEquals corresponds to lte()
type LessThanEquals struct {
	Value quad.Value `json:"value"`
}

// Type implements Step
func (s *LessThanEquals) Type() quad.IRI {
	return prefix + "LessThanEquals"
}

// GreaterThan corresponds to gt()
type GreaterThan struct {
	Value quad.Value `json:"value"`
}

// Type implements Step
func (s *GreaterThan) Type() quad.IRI {
	return prefix + "GreaterThan"
}

// GreaterThanEquals corresponds to gte()
type GreaterThanEquals struct {
	Value quad.Value `json:"value"`
}

// Type implements Step
func (s *GreaterThanEquals) Type() quad.IRI {
	return prefix + "GreaterThanEquals"
}

// RegExp corresponds to regex()
type RegExp struct {
	Expression  string `json:"expression"`
	IncludeIRIs bool   `json:"includeIRIs"`
}

// Type implements Step
func (s *RegExp) Type() quad.IRI {
	return prefix + "RegExp"
}

// Like corresponds to like()
type Like struct {
	Pattern string `json:"pattern"`
}

// Type implements Step
func (s *Like) Type() quad.IRI {
	return prefix + "Like"
}
