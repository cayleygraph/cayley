package schema

import (
	"context"
	"reflect"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/path"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc"
)

// GenerateID is called when any object without an ID field is being saved.
//
// Deprecated: see Config.GenerateID
var GenerateID = func(_ interface{}) quad.Value {
	return quad.RandomBlankNode()
}

var global = NewConfig()

// Global returns a default global schema config.
func Global() *Config {
	return global
}

// PathForType builds a path (morphism) for a given Go type.
//
// Deprecated: see Config.PathForType
func PathForType(rt reflect.Type) (*path.Path, error) {
	return global.PathForType(rt)
}

// WriteAsQuads writes a single value in form of quads into specified quad writer.
//
// Deprecated: see Config.WriteAsQuads
func WriteAsQuads(w quad.Writer, o interface{}) (quad.Value, error) {
	return global.WriteAsQuads(w, o)
}

// WriteNamespaces will writes namespaces list into graph.
//
// Deprecated: see Config.WriteNamespaces
func WriteNamespaces(w quad.Writer, n *voc.Namespaces) error {
	return global.WriteNamespaces(w, n)
}

// LoadNamespaces will load namespaces stored in graph to a specified list.
//
// Deprecated: see Config.LoadNamespaces
func LoadNamespaces(ctx context.Context, qs graph.QuadStore, dest *voc.Namespaces) error {
	return global.LoadNamespaces(ctx, qs, dest)
}

// LoadIteratorToDepth is the same as LoadIteratorTo, but stops at a specified depth.
//
// Deprecated: see Config.LoadIteratorToDepth
func LoadIteratorToDepth(ctx context.Context, qs graph.QuadStore, dst reflect.Value, depth int, list graph.Iterator) error {
	return global.LoadIteratorToDepth(ctx, qs, dst, depth, list)
}

// LoadIteratorTo is a lower level version of LoadTo.
//
// Deprecated: see Config.LoadIteratorTo
func LoadIteratorTo(ctx context.Context, qs graph.QuadStore, dst reflect.Value, list graph.Iterator) error {
	return global.LoadIteratorToDepth(ctx, qs, dst, -1, list)
}

// LoadPathTo is the same as LoadTo, but starts loading objects from a given path.
//
// Deprecated: see Config.LoadPathTo
func LoadPathTo(ctx context.Context, qs graph.QuadStore, dst interface{}, p *path.Path) error {
	return global.LoadIteratorTo(ctx, qs, reflect.ValueOf(dst), p.BuildIterator())
}

// LoadTo will load a sub-graph of objects starting from ids (or from any nodes, if empty)
// to a destination Go object. Destination can be a struct, slice or channel.
//
// Deprecated: see Config.LoadTo
func LoadTo(ctx context.Context, qs graph.QuadStore, dst interface{}, ids ...quad.Value) error {
	return global.LoadTo(ctx, qs, dst, ids...)
}

// LoadToDepth is the same as LoadTo, but stops at a specified depth.
//
// Deprecated: see Config.LoadToDepth
func LoadToDepth(ctx context.Context, qs graph.QuadStore, dst interface{}, depth int, ids ...quad.Value) error {
	return global.LoadToDepth(ctx, qs, dst, depth, ids...)
}
