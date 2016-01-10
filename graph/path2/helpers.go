package path

import (
	"github.com/google/cayley/graph"
)

var (
	_ Nodes         = NodeIteratorBuilder{}
	_ NodesReplacer = NodeIteratorBuilder{}

	_ Links         = LinkIteratorBuilder{}
	_ LinksReplacer = LinkIteratorBuilder{}

	_ Nodes         = Start{}
	_ NodesReplacer = Start{}
)

type NodeIteratorBuilder struct {
	Path    Nodes
	Builder func() graph.Iterator
}

func (f NodeIteratorBuilder) Optimize() (Nodes, bool)                        { return f, false }
func (f NodeIteratorBuilder) BuildIterator() graph.Iterator                  { return f.Builder() }
func (f NodeIteratorBuilder) Replace(_ WrapNodesFunc, _ WrapLinksFunc) Nodes { return f }

type LinkIteratorBuilder struct {
	Path    Links
	Builder func() graph.Iterator
}

func (f LinkIteratorBuilder) Optimize() (Links, bool)                        { return f, false }
func (f LinkIteratorBuilder) BuildIterator() graph.Iterator                  { return f.Builder() }
func (f LinkIteratorBuilder) Replace(_ WrapNodesFunc, _ WrapLinksFunc) Links { return f }

type Start struct{}

func (f Start) Optimize() (Nodes, bool)                        { return f, false }
func (f Start) BuildIterator() graph.Iterator                  { panic("build on morphism") }
func (f Start) Replace(_ WrapNodesFunc, _ WrapLinksFunc) Nodes { return f }

func Follow(from Nodes, via Nodes) Nodes {
	return Replace(via, func(p Nodes) (Nodes, bool) {
		if _, ok := p.(Start); ok {
			return from, false
		}
		return p, true
	}, nil).(Nodes)
}
