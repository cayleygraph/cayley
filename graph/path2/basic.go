package path

import "github.com/google/cayley/quad"

var _ NodePath = AllNodes{}

type AllNodes struct{}

func (p AllNodes) Simplify() (NodePath, bool) { return p, false }
func (p AllNodes) Optimize() (NodePath, bool) { return p, false }

var _ LinkPath = AllLinks{}

type AllLinks struct{}

func (p AllLinks) Simplify() (LinkPath, bool) { return p, false }
func (p AllLinks) Optimize() (LinkPath, bool) { return p, false }

var _ NodePath = FixedValues{}

type FixedValues struct {
	Values []string
}

func (p FixedValues) Simplify() (NodePath, bool) { return p, false }
func (p FixedValues) Optimize() (NodePath, bool) {
	if len(p.Values) == 0 {
		return nil, true
	}
	return p, false
}

var _ NodePath = HasA{}

type HasA struct {
	Links LinkPath
	Dir   quad.Direction
}

func (p HasA) Simplify() (NodePath, bool) { return p, false }
func (p HasA) Optimize() (NodePath, bool) {
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

var _ LinkPath = LinksTo{}

type LinksTo struct {
	Nodes NodePath
	Dir   quad.Direction
}

func (p LinksTo) Simplify() (LinkPath, bool) { return p, false }
func (p LinksTo) Optimize() (LinkPath, bool) {
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

var _ NodePath = NotNodes{}

type NotNodes struct {
	Nodes NodePath
}

func (p NotNodes) Simplify() (NodePath, bool) { return p, false }
func (p NotNodes) Optimize() (NodePath, bool) {
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

var _ NodePath = Tag{}

type Tag struct {
	Nodes NodePath
	Tags  []string
}

func (p Tag) Simplify() (NodePath, bool) { return p, false }
func (p Tag) Optimize() (NodePath, bool) {
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

var _ NodePath = IntersectNodes{}

type IntersectNodes []NodePath

func (p IntersectNodes) Simplify() (NodePath, bool) { return p, false }
func (p IntersectNodes) Optimize() (NodePath, bool) {
	if len(p) == 0 {
		return nil, true
	} else if len(p) == 1 {
		n, _ := p[0].Optimize()
		return n, true
	}
	nsets := make([]NodePath, 0, len(p))
	var optg bool
	for _, sp := range p {
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
	return IntersectNodes(nsets), optg
}

var _ NodePath = UnionNodes{}

type UnionNodes []NodePath

func (p UnionNodes) Simplify() (NodePath, bool) { return p, false }
func (p UnionNodes) Optimize() (NodePath, bool) {
	if len(p) == 0 {
		return nil, true
	} else if len(p) == 1 {
		n, _ := p[0].Optimize()
		return n, true
	}
	nsets := make([]NodePath, 0, len(p))
	var optg bool
	for _, sp := range p {
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

var _ LinkPath = IntersectLinks{}

type IntersectLinks []LinkPath

func (p IntersectLinks) Simplify() (LinkPath, bool) { return p, false }
func (p IntersectLinks) Optimize() (LinkPath, bool) {
	if len(p) == 0 {
		return nil, true
	} else if len(p) == 1 {
		n, _ := p[0].Optimize()
		return n, true
	}
	nsets := make([]LinkPath, 0, len(p))
	var optg bool
	for _, sp := range p {
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
	return IntersectLinks(nsets), optg
}

var _ LinkPath = UnionLinks{}

type UnionLinks []LinkPath

func (p UnionLinks) Simplify() (LinkPath, bool) { return p, false }
func (p UnionLinks) Optimize() (LinkPath, bool) {
	if len(p) == 0 {
		return nil, true
	} else if len(p) == 1 {
		n, _ := p[0].Optimize()
		return n, true
	}
	nsets := make([]LinkPath, 0, len(p))
	var optg bool
	for _, sp := range p {
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
