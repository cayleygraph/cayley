package path

import "github.com/google/cayley/quad"

type Except struct {
	Nodes NodePath
	From  NodePath
}

func (p Except) Simplify() (NodePath, bool) {
	return IntersectNodes{
		p.From,
		NotNodes{
			Nodes: p.Nodes,
		},
	}, true
}
func (p Except) Optimize() (NodePath, bool) {
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

type Out struct {
	From NodePath
	Via  NodePath
	Rev  bool
	Tags []string
}

func (p Out) Simplify() (NodePath, bool) {
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
	var label LinkPath
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
	}, true
}
func (p Out) Optimize() (NodePath, bool) {
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
