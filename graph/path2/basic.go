package path

import (
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
)

var _ Nodes = Tag{}

type Tag struct {
	Nodes Nodes
	Tags  []string
}

func (p Tag) Replace(nf WrapNodesFunc, _ WrapLinksFunc) Nodes {
	if nf == nil {
		return p
	}
	return Tag{Nodes: nf(p.Nodes), Tags: p.Tags}
}
func (p Tag) BuildIterator() graph.Iterator {
	it := p.Nodes.BuildIterator()
	tg := it.Tagger()
	for _, tag := range p.Tags {
		tg.Add(tag)
	}
	return it
}
func (p Tag) Simplify() (Nodes, bool) { return p, false }
func (p Tag) Optimize() (Nodes, bool) {
	if p.Nodes == nil {
		return nil, true
	}
	n, opt := p.Nodes.Optimize()
	if len(p.Tags) == 0 {
		return n, true
	} else if !opt {
		return p, false
	} else if n == nil {
		return nil, true
	}
	return Tag{
		Nodes: n,
		Tags:  p.Tags, // TODO: unique tags
	}, true
}

var (
	_ Nodes = IntersectNodes{}
)

type IntersectNodes []Nodes

func (p IntersectNodes) Replace(nf WrapNodesFunc, _ WrapLinksFunc) Nodes {
	if nf == nil {
		return p
	}
	nodes := make([]Nodes, len(p))
	for i := range p {
		nodes[i] = nf(p[i])
	}
	return IntersectNodes(nodes)
}
func (p IntersectNodes) BuildIterator() graph.Iterator {
	it := iterator.NewAnd(nil)
	for _, n := range p {
		it.AddSubIterator(n.BuildIterator())
	}
	return it
}
func (p IntersectNodes) Simplify() (Nodes, bool) { return p, false }
func (p IntersectNodes) Optimize() (Nodes, bool) {
	if len(p) == 0 {
		return nil, true
	} else if len(p) == 1 {
		n, _ := p[0].Optimize()
		return n, true
	}
	nsets := make([]Nodes, 0, len(p))
	var optg bool
	for _, sp := range p {
		if sp == nil {
			continue
		}
		n, opt := sp.Optimize()
		if n == nil { // intersect with zero = zero
			return nil, true
		} else if _, ok := n.(AllNodes); ok { // remove 'all' sets
			optg = true
			continue
		}
		optg = optg || opt
		nsets = append(nsets, n)
	}
	if len(nsets) == 0 {
		return nil, true
	} else if len(nsets) == 1 {
		return nsets[0], true
	}
	// TODO: intersect FixedValues into a single one
	// TODO: all optimizations from iterator/and_iterator_optimize.go
	return IntersectNodes(nsets), optg
}

var _ Nodes = UnionNodes{}

type UnionNodes []Nodes

func (p UnionNodes) Replace(nf WrapNodesFunc, _ WrapLinksFunc) Nodes {
	if nf == nil {
		return p
	}
	nodes := make([]Nodes, len(p))
	for i := range p {
		nodes[i] = nf(p[i])
	}
	return UnionNodes(nodes)
}
func (p UnionNodes) BuildIterator() graph.Iterator {
	it := iterator.NewOr()
	for _, n := range p {
		it.AddSubIterator(n.BuildIterator())
	}
	return it
}
func (p UnionNodes) Simplify() (Nodes, bool) { return p, false }
func (p UnionNodes) Optimize() (Nodes, bool) {
	if len(p) == 0 {
		return nil, true
	} else if len(p) == 1 {
		n, _ := p[0].Optimize()
		return n, true
	}
	nsets := make([]Nodes, 0, len(p))
	var optg bool
	for _, sp := range p {
		if sp == nil {
			continue
		}
		n, opt := sp.Optimize()
		if _, ok := n.(AllNodes); ok { // intersect with all = all
			return AllNodes{}, true
		} else if n == nil { // remove empty sets
			optg = true
			continue
		}
		optg = optg || opt
		nsets = append(nsets, n)
	}
	if len(nsets) == 0 {
		return nil, true
	} else if len(nsets) == 1 {
		return nsets[0], true
	}
	// TODO: deduplicate FixedValues into a single one
	return UnionNodes(nsets), optg
}

var (
	_ Links = IntersectLinks{}
)

type IntersectLinks []Links

func (p IntersectLinks) Replace(_ WrapNodesFunc, lf WrapLinksFunc) Links {
	if lf == nil {
		return p
	}
	nodes := make([]Links, len(p))
	for i := range p {
		nodes[i] = lf(p[i])
	}
	return IntersectLinks(nodes)
}
func (p IntersectLinks) BuildIterator() graph.Iterator {
	it := iterator.NewAnd(nil)
	for _, n := range p {
		it.AddSubIterator(n.BuildIterator())
	}
	return it
}
func (p IntersectLinks) Simplify() (Links, bool) { return p, false }
func (p IntersectLinks) Optimize() (Links, bool) {
	if len(p) == 0 {
		return nil, true
	} else if len(p) == 1 {
		n, _ := p[0].Optimize()
		return n, true
	}
	nsets := make([]Links, 0, len(p))
	var optg bool
	for _, sp := range p {
		if sp == nil {
			continue
		}
		n, opt := sp.Optimize()
		if n == nil { // intersect with zero = zero
			return nil, true
		} else if _, ok := n.(AllLinks); ok { // remove 'all' sets
			optg = true
			continue
		}
		optg = optg || opt
		nsets = append(nsets, n)
	}
	if len(nsets) == 0 {
		return nil, true
	} else if len(nsets) == 1 {
		return nsets[0], true
	}
	// TODO: intersect FixedValues into a single one
	// TODO: all optimizations from iterator/and_iterator_optimize.go
	return IntersectLinks(nsets), optg
}

var _ Links = UnionLinks{}

type UnionLinks []Links

func (p UnionLinks) Replace(_ WrapNodesFunc, lf WrapLinksFunc) Links {
	if lf == nil {
		return p
	}
	nodes := make([]Links, len(p))
	for i := range p {
		nodes[i] = lf(p[i])
	}
	return UnionLinks(nodes)
}
func (p UnionLinks) BuildIterator() graph.Iterator {
	it := iterator.NewOr()
	for _, n := range p {
		it.AddSubIterator(n.BuildIterator())
	}
	return it
}
func (p UnionLinks) Simplify() (Links, bool) { return p, false }
func (p UnionLinks) Optimize() (Links, bool) {
	if len(p) == 0 {
		return nil, true
	} else if len(p) == 1 {
		n, _ := p[0].Optimize()
		return n, true
	}
	nsets := make([]Links, 0, len(p))
	var optg bool
	for _, sp := range p {
		if sp == nil {
			continue
		}
		n, opt := sp.Optimize()
		if _, ok := n.(AllLinks); ok { // intersect with all = all
			return AllLinks{}, true
		} else if n == nil { // remove empty sets
			optg = true
			continue
		}
		optg = optg || opt
		nsets = append(nsets, n)
	}
	if len(nsets) == 0 {
		return nil, true
	} else if len(nsets) == 1 {
		return nsets[0], true
	}
	// TODO: deduplicate FixedValues into a single one
	return UnionLinks(nsets), optg
}
