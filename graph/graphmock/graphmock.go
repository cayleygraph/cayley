package graphmock

import (
	"context"
	"strconv"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/quad"
)

var (
	_ graph.Ref = IntVal(0)
	_ graph.Ref = StringNode("")
)

type IntVal int

func (v IntVal) Key() interface{} { return v }

type StringNode string

func (s StringNode) Key() interface{} { return s }

// Oldstore is a mocked version of the QuadStore interface, for use in tests.
type Oldstore struct {
	Parse bool
	Data  []string
	Iter  graph.Iterator
}

func (qs *Oldstore) valueAt(i int) quad.Value {
	if !qs.Parse {
		return quad.Raw(qs.Data[i])
	}
	iv, err := strconv.Atoi(qs.Data[i])
	if err == nil {
		return quad.Int(iv)
	}
	return quad.String(qs.Data[i])
}

func (qs *Oldstore) ValueOf(s quad.Value) graph.Ref {
	if s == nil {
		return nil
	}
	for i := range qs.Data {
		if va := qs.valueAt(i); va != nil && s.String() == va.String() {
			return iterator.Int64Node(i)
		}
	}
	return nil
}

func (qs *Oldstore) NewQuadWriter() (quad.WriteCloser, error) {
	return nopWriter{}, nil
}

type nopWriter struct{}

func (nopWriter) WriteQuad(q quad.Quad) error {
	return nil
}

func (nopWriter) WriteQuads(buf []quad.Quad) (int, error) {
	return len(buf), nil
}

func (nopWriter) Close() error {
	return nil
}

func (qs *Oldstore) ApplyDeltas([]graph.Delta, graph.IgnoreOpts) error { return nil }

func (qs *Oldstore) Quad(graph.Ref) quad.Quad { return quad.Quad{} }

func (qs *Oldstore) QuadIterator(d quad.Direction, i graph.Ref) graph.Iterator {
	return qs.Iter
}

func (qs *Oldstore) QuadIteratorSize(ctx context.Context, d quad.Direction, val graph.Ref) (graph.Size, error) {
	sz, exact := qs.Iter.Size()
	return graph.Size{Size: sz, Exact: exact}, nil
}

func (qs *Oldstore) NodesAllIterator() graph.Iterator { return &iterator.Null{} }

func (qs *Oldstore) QuadsAllIterator() graph.Iterator { return &iterator.Null{} }

func (qs *Oldstore) NameOf(v graph.Ref) quad.Value {
	switch v := v.(type) {
	case iterator.Int64Node:
		i := int(v)
		if i < 0 || i >= len(qs.Data) {
			return nil
		}
		return qs.valueAt(i)
	case StringNode:
		if qs.Parse {
			return quad.String(v)
		}
		return quad.Raw(string(v))
	default:
		return nil
	}
}

func (qs *Oldstore) Size() int64 { return 0 }

func (qs *Oldstore) DebugPrint() {}

func (qs *Oldstore) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	return &iterator.Null{}, false
}

func (qs *Oldstore) Close() error { return nil }

func (qs *Oldstore) QuadDirection(graph.Ref, quad.Direction) graph.Ref {
	return iterator.Int64Node(0)
}

func (qs *Oldstore) RemoveQuad(t quad.Quad) {}

func (qs *Oldstore) Type() string { return "oldmockstore" }

type Store struct {
	Data []quad.Quad
}

var _ graph.QuadStore = &Store{}

func (qs *Store) ValueOf(s quad.Value) graph.Ref {
	for _, q := range qs.Data {
		if q.Subject == s || q.Object == s {
			return graph.PreFetched(s)
		}
	}
	return nil
}

func (qs *Store) ApplyDeltas([]graph.Delta, graph.IgnoreOpts) error { return nil }

func (qs *Store) NewQuadWriter() (quad.WriteCloser, error) {
	return nopWriter{}, nil
}

type quadValue struct {
	q quad.Quad
}

func (q quadValue) Key() interface{} {
	return q.q.String()
}

func (qs *Store) Quad(v graph.Ref) quad.Quad { return v.(quadValue).q }

func (qs *Store) NameOf(v graph.Ref) quad.Value {
	if v == nil {
		return nil
	}
	return v.(graph.PreFetchedValue).NameOf()
}

func (qs *Store) RemoveQuad(t quad.Quad) {}

func (qs *Store) Type() string { return "mockstore" }

func (qs *Store) QuadDirection(v graph.Ref, d quad.Direction) graph.Ref {
	return graph.PreFetched(qs.Quad(v).Get(d))
}

func (qs *Store) Close() error { return nil }

func (qs *Store) DebugPrint() {}

func (qs *Store) QuadIterator(d quad.Direction, i graph.Ref) graph.Iterator {
	fixed := iterator.NewFixed()
	v := i.(graph.PreFetchedValue).NameOf()
	for _, q := range qs.Data {
		if q.Get(d) == v {
			fixed.Add(quadValue{q})
		}
	}
	return fixed
}

func (qs *Store) QuadIteratorSize(ctx context.Context, d quad.Direction, val graph.Ref) (graph.Size, error) {
	v := val.(graph.PreFetchedValue).NameOf()
	sz := graph.Size{Exact: true}
	for _, q := range qs.Data {
		if q.Get(d) == v {
			sz.Size++
		}
	}
	return sz, nil
}

func (qs *Store) NodesAllIterator() graph.Iterator {
	set := make(map[string]bool)
	for _, q := range qs.Data {
		for _, d := range quad.Directions {
			n := qs.NameOf(graph.PreFetched(q.Get(d)))
			if n != nil {
				set[n.String()] = true
			}
		}
	}
	fixed := iterator.NewFixed()
	for k := range set {
		fixed.Add(graph.PreFetched(quad.Raw(k)))
	}
	return fixed
}

func (qs *Store) QuadsAllIterator() graph.Iterator {
	fixed := iterator.NewFixed()
	for _, q := range qs.Data {
		fixed.Add(quadValue{q})
	}
	return fixed
}

func (qs *Store) Stats(ctx context.Context, exact bool) (graph.Stats, error) {
	set := make(map[string]struct{})
	for _, q := range qs.Data {
		for _, d := range quad.Directions {
			n := qs.NameOf(graph.PreFetched(q.Get(d)))
			if n != nil {
				set[n.String()] = struct{}{}
			}
		}
	}
	return graph.Stats{
		Nodes: graph.Size{Size: int64(len(set)), Exact: true},
		Quads: graph.Size{Size: int64(len(qs.Data)), Exact: true},
	}, nil
}
