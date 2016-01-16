package path

import (
	"fmt"
	"github.com/google/cayley/graph"
)

// PathObj is an common interface for path (for both Links and Nodes).
type PathObj interface {
	// BuildIterator creates an iterator for the given path.
	//
	// Note, that some path parts may be abstract (All, for example),
	// thus it must be bound to QuadStore with BindTo or OptimizeOn.
	BuildIterator() graph.Iterator
}

// NodesWrapper is a function which can replace Nodes.
type NodesWrapper func(Nodes) Nodes

// LinksWrapper is a function which can replace Links.
type LinksWrapper func(Links) Links

// Nodes is an interface for path which describes a set of nodes (values).
type Nodes interface {
	PathObj
	// Replace applies wrapper functions to all sub-paths and returns
	// a modified copy of this path.
	//
	// One of the wrappers can be nil, what means that paths of this
	// type should not be changed.
	Replace(nf NodesWrapper, lf LinksWrapper) Nodes
	// Optimize returns a copy of current path with all possible optimizations applied.
	//
	// The second return value is true if path was replaced.
	//
	// It is a path's responsibility to apply Optimize to all it's sub-paths.
	Optimize() (Nodes, bool)
}

// Links is an interface for path which describes a set of links (quads).
type Links interface {
	PathObj
	// Replace applies wrapper functions to all sub-paths and returns
	// a modified copy of this path.
	//
	// One of the wrappers can be nil, what means that paths of this
	// type should not be changed.
	Replace(nf NodesWrapper, lf LinksWrapper) Links
	// Optimize returns a copy of current path with all possible optimizations applied.
	//
	// The second return value is true if path was replaced.
	//
	// It is a path's responsibility to apply Optimize to all it's sub-paths.
	Optimize() (Links, bool)
}

// PathOptimizer is an optional interface for QuadStore, which
// advertise an ability to optimize Nodes or Links paths.
//
// TODO: make it a required interface and remove abstract path replacer.
type PathOptimizer interface {
	// OptimizeNodesPath is a QuadStore's variant of Nodes.Optimize.
	OptimizeNodesPath(p Nodes) (Nodes, bool)
	// OptimizeLinksPath is a QuadStore's variant of Links.Optimize.
	OptimizeLinksPath(p Links) (Links, bool)
}

// NodesAbstract is an interface for Nodes paths that can not be used without
// QuadStore implementation (All, for example).
type NodesAbstract interface {
	// BindTo returns a Nodes path bound to provided QuadStore.
	BindTo(qs graph.QuadStore) Nodes
}

// LinksAbstract is an interface for Links paths that can not be used without
// QuadStore implementation (All, for example).
type LinksAbstract interface {
	// BindTo returns a Links path bound to provided QuadStore.
	BindTo(qs graph.QuadStore) Links
}

// NodesSimplifier is an optional interface for high-level path objects,
// that can be decomposed into a tree of basic paths.
type NodesSimplifier interface {
	Simplify() Nodes
}

// LinksSimplifier is an optional interface for high-level path objects,
// that can be decomposed into a tree of basic paths.
type LinksSimplifier interface {
	Simplify() Links
}

// NodesReverser is an optional interface for Node paths that
// traversal direction can be reversed.
type NodesReverser interface {
	Reverse() Nodes
}

type ReplaceNodesFunc func(p Nodes) (np Nodes, next bool)
type ReplaceLinksFunc func(p Links) (np Links, next bool)

// Replace will call provided functions to replace Nodes and/or Links in the path
// recursively.
//
// It stops tree descent if function returns false as a second return value.
//
// One of the functions can be nil.
func Replace(p PathObj, nf ReplaceNodesFunc, lf ReplaceLinksFunc) (out PathObj) {
	if p == nil {
		return nil
	} else if nf == nil && lf == nil {
		return p
	}
	//	fmt.Printf("replace on %T\n", p)
	//	defer func(){
	//		fmt.Printf("replaced %T -> %T\n", p, out)
	//	}()
	switch tp := p.(type) {
	case Nodes:
		if tp != nil {
			return replaceNodes(tp, nf, lf)
		}
		return nil
	case Links:
		if tp != nil {
			return replaceLinks(tp, nf, lf)
		}
		return nil
	default:
		panic(fmt.Errorf("unknown path type: %T", p))
	}
}

func replaceNodes(p Nodes, nf ReplaceNodesFunc, lf ReplaceLinksFunc) Nodes {
	if p == nil {
		return nil
	}
	if nf != nil {
		var next bool
		p, next = nf(p)
		if !next {
			return p
		} else if p == nil {
			return nil
		}
	}
	var (
		nw NodesWrapper
		lw LinksWrapper
	)
	if nf != nil {
		nw = func(p Nodes) Nodes { return replaceNodes(p, nf, lf) }
	}
	if lf != nil {
		lw = func(p Links) Links { return replaceLinks(p, nf, lf) }
	}
	return p.Replace(nw, lw)
}
func replaceLinks(p Links, nf ReplaceNodesFunc, lf ReplaceLinksFunc) Links {
	if p == nil {
		return nil
	}
	if lf != nil {
		var next bool
		p, next = lf(p)
		if !next {
			return p
		} else if p == nil {
			return nil
		}
	}
	var (
		nw NodesWrapper
		lw LinksWrapper
	)
	if nf != nil {
		nw = func(p Nodes) Nodes { return replaceNodes(p, nf, lf) }
	}
	if lf != nil {
		lw = func(p Links) Links { return replaceLinks(p, nf, lf) }
	}
	return p.Replace(nw, lw)
}

// Optimize applies all possible optimizations, that are common for all QuadStores.
//
// Second return value is true, if path was replaced.
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

// OptimizeOn binds provided path to a QuadStore, applying all
// generic optimizations an all QuadStore-specific optimizations.
//
// Resulting path will always produce the fastest possible iterator
// for a specified query shape for that particular QuadStore.
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
			if opt { // replaced, stop tree descent
				return np, false
			}
			if sm, ok := sp.(NodesSimplifier); ok {
				np, _ = sm.Simplify().Optimize()
			} else { // can't simplify, continue descent
				return sp, true
			}
			// give QS a second chance - optimize a simplified variant
			np, opt = po.OptimizeNodesPath(np)
			return np, !opt
		}, func(sp Links) (Links, bool) {
			np, opt := po.OptimizeLinksPath(sp)
			if opt { // replaced, stop tree descent
				return np, false
			}
			if sm, ok := sp.(LinksSimplifier); ok {
				np, _ = sm.Simplify().Optimize()
			} else { // can't simplify, continue descent
				return sp, true
			}
			// give QS a second chance - optimize a simplified variant
			np, opt = po.OptimizeLinksPath(np)
			return np, !opt
		})
	}
	// Step 3: replace all abstract paths
	// TODO: remove this when all stores will support direct abstract path optimizations
	p = bindTo(p, qs, true)
	return p
}

// BindTo binds all abstract sub-paths to QuadStore, making path buildable.
//
// BindTo is similar to OptimizeOn, but it omits all generic optimization.
// This may be useful for rarely-reused queries, where optimization time
// is comparable to iteration over non-optimized iterator.
func BindTo(p PathObj, qs graph.QuadStore) PathObj {
	return bindTo(p, qs, false)
}

func bindTo(p PathObj, qs graph.QuadStore, optimize bool) PathObj {
	return Replace(p, func(sp Nodes) (Nodes, bool) {
		if a, ok := sp.(NodesAbstract); ok {
			return a.BindTo(qs), false
		}
		if si, ok := sp.(NodesSimplifier); ok {
			sp = si.Simplify()
			if optimize {
				sp, _ = sp.Optimize()
			}
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
			if optimize {
				sp, _ = sp.Optimize()
			}
		}
		if a, ok := sp.(LinksAbstract); ok {
			return a.BindTo(qs), false
		}
		return sp, true
	})
}
