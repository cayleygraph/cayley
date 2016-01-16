package path

import (
	"github.com/google/cayley/graph"
)

var (
	_ Nodes = NodeIteratorBuilder{}
	_ Links = LinkIteratorBuilder{}
	_ Nodes = Start{}
)

type NodeIteratorBuilder struct {
	Path    Nodes
	Builder func() graph.Iterator
}

func (f NodeIteratorBuilder) Optimize() (Nodes, bool)                      { return f, false }
func (f NodeIteratorBuilder) BuildIterator() graph.Iterator                { return f.Builder() }
func (f NodeIteratorBuilder) Replace(_ NodesWrapper, _ LinksWrapper) Nodes { return f }

type LinkIteratorBuilder struct {
	Path    Links
	Builder func() graph.Iterator
}

func (f LinkIteratorBuilder) Optimize() (Links, bool)                      { return f, false }
func (f LinkIteratorBuilder) BuildIterator() graph.Iterator                { return f.Builder() }
func (f LinkIteratorBuilder) Replace(_ NodesWrapper, _ LinksWrapper) Links { return f }

type Start struct{}

func (f Start) Optimize() (Nodes, bool)                      { return f, false }
func (f Start) BuildIterator() graph.Iterator                { panic("build on morphism") }
func (f Start) Replace(_ NodesWrapper, _ LinksWrapper) Nodes { return f }

func Follow(from Nodes, via Nodes) Nodes {
	return Replace(via, func(p Nodes) (Nodes, bool) {
		if _, ok := p.(Start); ok {
			return from, false
		}
		return p, true
	}, nil).(Nodes)
}

func FollowReverse(from Nodes, via Nodes) Nodes {
	return Replace(via, func(p Nodes) (Nodes, bool) {
		if _, ok := p.(Start); ok {
			return from, false
		} else if rev, ok := p.(NodesReverser); ok {
			return rev.Reverse(), true
		}
		return p, true
	}, nil).(Nodes)
}
