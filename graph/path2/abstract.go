package path

import (
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

var (
	_ Nodes         = AllNodes{}
	_ NodesAbstract = AllNodes{}
)

type AllNodes struct{}

func (p AllNodes) BuildIterator() graph.Iterator                  { panic("build on abstract path: AllNodes") }
func (p AllNodes) Replace(_ WrapNodesFunc, _ WrapLinksFunc) Nodes { return p }
func (p AllNodes) BindTo(qs graph.QuadStore) Nodes {
	return NodeIteratorBuilder{
		Path: p,
		Builder: func() graph.Iterator {
			return qs.NodesAllIterator()
		},
	}
}
func (p AllNodes) Optimize() (Nodes, bool) { return p, false }

var (
	_ Links         = AllLinks{}
	_ LinksAbstract = AllLinks{}
)

type AllLinks struct{}

func (p AllLinks) BuildIterator() graph.Iterator                  { panic("build on abstract path: AllLinks") }
func (p AllLinks) Replace(_ WrapNodesFunc, _ WrapLinksFunc) Links { return p }
func (p AllLinks) BindTo(qs graph.QuadStore) Links {
	return LinkIteratorBuilder{
		Path: p,
		Builder: func() graph.Iterator {
			return qs.QuadsAllIterator()
		},
	}
}
func (p AllLinks) Optimize() (Links, bool) { return p, false }

var (
	_ Nodes         = Fixed{}
	_ NodesAbstract = Fixed{}
)

type Fixed []string

func (p Fixed) BuildIterator() graph.Iterator                  { panic("build on abstract path: FixedValues") }
func (p Fixed) Replace(_ WrapNodesFunc, _ WrapLinksFunc) Nodes { return p }
func (p Fixed) BindTo(qs graph.QuadStore) Nodes {
	return NodeIteratorBuilder{
		Path: p,
		Builder: func() graph.Iterator {
			it := qs.FixedIterator()
			for _, v := range p {
				if gv := qs.ValueOf(v); gv != nil {
					it.Add(gv)
				}
			}
			return it
		},
	}
}
func (p Fixed) Optimize() (Nodes, bool) {
	if len(p) == 0 {
		return nil, true
	}
	return p, false
}

var (
	_ Nodes         = HasA{}
	_ NodesAbstract = HasA{}
)

type HasA struct {
	Links Links
	Dir   quad.Direction
}

func (p HasA) BuildIterator() graph.Iterator { panic("build on abstract path: HasA") }
func (p HasA) Replace(_ WrapNodesFunc, lf WrapLinksFunc) Nodes {
	if lf == nil {
		return p
	}
	return HasA{Links: lf(p.Links), Dir: p.Dir}
}
func (p HasA) BindTo(qs graph.QuadStore) Nodes {
	np := OptimizeOn(p.Links, qs)
	return NodeIteratorBuilder{
		Path: p,
		Builder: func() graph.Iterator {
			return iterator.NewHasA(qs, np.BuildIterator(), p.Dir)
		},
	}
}
func (p HasA) Optimize() (Nodes, bool) {
	if p.Links == nil {
		return nil, true
	}
	n, opt := p.Links.Optimize()
	if !opt {
		return p, false
	} else if n == nil {
		return nil, true
	}
	return HasA{
		Links: n,
		Dir:   p.Dir,
	}, true
}

var (
	_ Links         = LinksTo{}
	_ LinksAbstract = LinksTo{}
)

type LinksTo struct {
	Nodes Nodes
	Dir   quad.Direction
}

func (p LinksTo) BuildIterator() graph.Iterator { panic("build on abstract path: LinksTo") }
func (p LinksTo) Replace(nf WrapNodesFunc, _ WrapLinksFunc) Links {
	if nf == nil {
		return p
	}
	return LinksTo{Nodes: nf(p.Nodes), Dir: p.Dir}
}
func (p LinksTo) BindTo(qs graph.QuadStore) Links {
	np := OptimizeOn(p.Nodes, qs)
	return LinkIteratorBuilder{
		Path: p,
		Builder: func() graph.Iterator {
			return iterator.NewLinksTo(qs, np.BuildIterator(), p.Dir)
		},
	}
}
func (p LinksTo) Optimize() (Links, bool) {
	if p.Nodes == nil {
		return nil, true
	}
	n, opt := p.Nodes.Optimize()
	if !opt {
		return p, false
	} else if n == nil {
		return nil, true
	}
	return LinksTo{
		Nodes: n,
		Dir:   p.Dir,
	}, true
}

var (
	_ Nodes         = NotNodes{}
	_ NodesAbstract = NotNodes{}
)

type NotNodes struct {
	Nodes Nodes
}

func (p NotNodes) BuildIterator() graph.Iterator { panic("build on abstract path: NotNodes") }
func (p NotNodes) Replace(nf WrapNodesFunc, _ WrapLinksFunc) Nodes {
	if nf == nil {
		return p
	}
	return NotNodes{Nodes: nf(p.Nodes)}
}
func (p NotNodes) BindTo(qs graph.QuadStore) Nodes {
	np := OptimizeOn(p.Nodes, qs)
	return NodeIteratorBuilder{
		Path: p,
		Builder: func() graph.Iterator {
			return iterator.NewNot(np.BuildIterator(), qs.NodesAllIterator())
		},
	}
}
func (p NotNodes) Optimize() (Nodes, bool) {
	if p.Nodes == nil {
		return AllNodes{}, true
	}
	n, opt := p.Nodes.Optimize()
	switch t := n.(type) {
	case AllNodes:
		return nil, true
	case NotNodes:
		n, _ := t.Nodes.Optimize()
		return n, true
	}
	return NotNodes{Nodes: n}, opt
}
