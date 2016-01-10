package path

import (
	"fmt"
	"github.com/google/cayley/graph"
)

type PathObj interface {
	//DescribePath()
	BuildIterator() graph.Iterator
}

type PathOptimizer interface {
	OptimizeNodesPath(p Nodes) (Nodes, bool)
	OptimizeLinksPath(p Links) (Links, bool)
}

type NodesAbstract interface {
	BindTo(qs graph.QuadStore) Nodes
}

type LinksAbstract interface {
	BindTo(qs graph.QuadStore) Links
}

type NodesSimplifier interface {
	Simplify() Nodes
}

type LinksSimplifier interface {
	Simplify() Links
}

type Nodes interface {
	PathObj
	NodesReplacer
	Optimize() (Nodes, bool)
}
type Links interface {
	PathObj
	LinksReplacer
	Optimize() (Links, bool)
}

//

type WrapNodesFunc func(Nodes) Nodes
type WrapLinksFunc func(Links) Links

type ReplaceNodesFunc func(p Nodes) (np Nodes, next bool)
type ReplaceLinksFunc func(p Links) (np Links, next bool)
type NodesReplacer interface {
	Replace(nf WrapNodesFunc, lf WrapLinksFunc) Nodes
}
type LinksReplacer interface {
	Replace(nf WrapNodesFunc, lf WrapLinksFunc) Links
}

func replaceAny(p PathObj, nf ReplaceNodesFunc, lf ReplaceLinksFunc) PathObj {
	// TODO: reflect-based replacer
	panic(fmt.Errorf("not implemented, type: %T", p))
}
func Replace(p PathObj, nf ReplaceNodesFunc, lf ReplaceLinksFunc) (out PathObj) {
	if p == nil {
		return nil
	}
	//	fmt.Printf("replace on %T\n", p)
	//	defer func(){
	//		fmt.Printf("replaced %T -> %T\n", p, out)
	//	}()
	switch tp := p.(type) {
	case Nodes:
		if nf != nil && tp != nil {
			np, next := nf(tp)
			if !next {
				return np
			}
			p = np
		}
	case Links:
		if lf != nil && tp != nil {
			np, next := lf(tp)
			if !next {
				return np
			}
			p = np
		}
	default:
		panic("unknown type")
	}
	nfw := func(p Nodes) Nodes {
		return Replace(p, nf, lf).(Nodes)
	}
	lfw := func(p Links) Links {
		return Replace(p, nf, lf).(Links)
	}
	if nr, ok := p.(NodesReplacer); ok {
		return nr.Replace(nfw, lfw)
	} else if lr, ok := p.(LinksReplacer); ok {
		return lr.Replace(nfw, lfw)
	} else {
		return replaceAny(p, nf, lf)
	}
}

//

func Optimize(p PathObj) (PathObj, bool) {
	if p == nil {
		return nil, false
	}
	switch tp := p.(type) {
	case Nodes:
		if tp == nil {
			return nil, true
		}
		return tp.Optimize()
	case Links:
		if tp == nil {
			return nil, true
		}
		return tp.Optimize()
	}
	return p, false
}

// Steps:
// 1) Optimize everything
// 2) Replace all Abstract paths
// 3) Give QS a chance to optimize
//   a) Check for known types and replace them
//   b) If type is unknown, try to simplify
//   c) If it's simplifiable - try to optimize again
//   d) If failed, leave it untouched

func OptimizeOn(p PathObj, qs graph.QuadStore) PathObj {
	// Step 1: optimize pure path
	p, _ = Optimize(p)
	if qs == nil {
		return p
	}
	// Step 2: let quad store optimize things
	if po, ok := qs.(PathOptimizer); ok {
		p = Replace(p, func(sp Nodes) (Nodes, bool) {
			np, opt := po.OptimizeNodesPath(sp)
			if opt { // replaced, no need to indirect
				return np, false
			}
			if sm, ok := sp.(NodesSimplifier); ok {
				np = sm.Simplify()
			} else { // can't simplify, indirect further
				return sp, true
			}
			np, opt = po.OptimizeNodesPath(np)
			if !opt { // can't optimize simplified variant
				return sp, true
			}
			return np, false
		}, func(sp Links) (Links, bool) {
			np, ok := po.OptimizeLinksPath(sp)
			if ok { // replaced, no need to indirect
				return np, false
			}
			if sm, ok := sp.(LinksSimplifier); ok {
				np = sm.Simplify()
			} else { // can't simplify, indirect further
				return sp, true
			}
			np, ok = po.OptimizeLinksPath(np)
			if !ok { // can't optimize simplified variant
				return sp, true
			}
			return np, false
		})
	}
	// Step 3: replace all abstract paths
	p = Replace(p, func(sp Nodes) (Nodes, bool) {
		if a, ok := sp.(NodesAbstract); ok {
			return a.BindTo(qs), false
		}
		if si, ok := sp.(NodesSimplifier); ok {
			sp = si.Simplify()
		}
		if a, ok := sp.(NodesAbstract); ok {
			return a.BindTo(qs), false
		}
		return sp, true
	}, func(sp Links) (Links, bool) {
		if a, ok := sp.(LinksAbstract); ok {
			return a.BindTo(qs), false
		}
		if si, ok := sp.(LinksSimplifier); ok {
			sp = si.Simplify()
		}
		if a, ok := sp.(LinksAbstract); ok {
			return a.BindTo(qs), false
		}
		return sp, true
	})
	return p
}
