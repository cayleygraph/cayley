package linkedql

import "github.com/cayleygraph/cayley/query/path"

// Operator represents an operator used in a query inside a step (e.g. greater than).
type Operator interface {
	RegistryItem
	Apply(p *path.Path) (*path.Path, error)
}
