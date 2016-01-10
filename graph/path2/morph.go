package path

import (
	"github.com/google/cayley/graph"
	"github.com/google/cayley/quad"
)

var (
	_ Nodes           = Except{}
	_ NodesSimplifier = Except{}
)

type Except struct {
	Nodes Nodes
	From  Nodes
}

func (p Except) Replace(nf WrapNodesFunc, _ WrapLinksFunc) Nodes {
	if nf == nil {
		return p
	}
	return Except{
		Nodes: nf(p.Nodes), From: nf(p.From),
	}
}
func (p Except) BuildIterator() graph.Iterator {
	return p.Simplify().BuildIterator()
}
func (p Except) Simplify() Nodes {
	return IntersectNodes{
		p.From,
		NotNodes{
			Nodes: p.Nodes,
		},
	}
}
func (p Except) Optimize() (Nodes, bool) {
	if p.From == nil {
		return nil, true
	}
	nf, optf := p.From.Optimize()
	if nf == nil {
		return nil, true
	} else if p.Nodes == nil {
		return nf, true
	}
	n, opt := p.Nodes.Optimize()
	if n == nil {
		return nf, true
	} else if _, ok := n.(AllNodes); ok { // except all = zero
		return nil, true
	} else if _, ok = nf.(AllNodes); ok { // except from all = not nodes
		return NotNodes{Nodes: n}, true
	}
	return Except{
		Nodes: n,
		From:  nf,
	}, opt || optf
}

var (
	_ Nodes           = Out{}
	_ NodesSimplifier = Out{}
)

type Out struct {
	From Nodes
	Via  Nodes
	Rev  bool
	Tags []string
}

func (p Out) Replace(nf WrapNodesFunc, nl WrapLinksFunc) Nodes {
	if nf == nil {
		return p
	}
	return Out{
		From: nf(p.From),
		Via:  nf(p.Via),
		Rev:  p.Rev,
		Tags: p.Tags,
	}
}
func (p Out) BuildIterator() graph.Iterator {
	return p.Simplify().BuildIterator()
}
func (p Out) Simplify() Nodes {
	start, goal := quad.Subject, quad.Object
	if p.Rev {
		start, goal = goal, start
	}
	source := LinksTo{
		Nodes: p.From,
		Dir:   start,
	}
	trail := LinksTo{
		Nodes: Tag{
			Nodes: p.Via,
			Tags:  p.Tags,
		},
		Dir: quad.Predicate,
	}
	var label Links
	// TODO: labels
	//	if ctx != nil {
	//		if ctx.labelSet != nil {
	//			labeliter := ctx.labelSet.BuildIteratorOn(viaPath.qs)
	//			label = iterator.NewLinksTo(viaPath.qs, labeliter, quad.Label)
	//		}
	//	}
	return HasA{
		Links: IntersectLinks{
			source,
			trail,
			label,
		},
		Dir: goal,
	}
}
func (p Out) Optimize() (Nodes, bool) {
	if p.From == nil || p.Via == nil {
		return nil, true
	}
	nf, fopt := p.From.Optimize()
	if p.From == nil {
		return nil, true
	}
	nv, vopt := p.Via.Optimize()
	if p.Via == nil {
		return nil, true
	}
	return Out{
		From: nf, Via: nv,
		Rev:  p.Rev,
		Tags: p.Tags, // TODO: unique tags
	}, fopt || vopt
}
