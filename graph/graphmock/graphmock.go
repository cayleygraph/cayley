package graphmock

import (
	"strconv"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
)

var (
	_ graph.Value = IntVal(0)
	_ graph.Value = StringNode("")
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

func (qs *Oldstore) ValueOf(s quad.Value) graph.Value {
	if s == nil {
		return nil
	}
	for i := range qs.Data {
		if s.String() == qs.valueAt(i).String() {
			return iterator.Int64Node(i)
		}
	}
	return nil
}

func (qs *Oldstore) ApplyDeltas([]graph.Delta, graph.IgnoreOpts) error { return nil }

func (qs *Oldstore) Quad(graph.Value) quad.Quad { return quad.Quad{} }

func (qs *Oldstore) QuadIterator(d quad.Direction, i graph.Value) graph.Iterator {
	return qs.Iter
}

func (qs *Oldstore) NodesAllIterator() graph.Iterator { return &iterator.Null{} }

func (qs *Oldstore) QuadsAllIterator() graph.Iterator { return &iterator.Null{} }

func (qs *Oldstore) NameOf(v graph.Value) quad.Value {
	switch v.(type) {
	case iterator.Int64Node:
		i := int(v.(iterator.Int64Node))
		if i < 0 || i >= len(qs.Data) {
			return nil
		}
		return qs.valueAt(i)
	case StringNode:
		if qs.Parse {
			return quad.String(v.(StringNode))
		}
		return quad.Raw(v.(StringNode))
	default:
		return nil
	}
}

func (qs *Oldstore) Size() int64 { return 0 }

func (qs *Oldstore) Horizon() graph.PrimaryKey { return graph.NewSequentialKey(0) }

func (qs *Oldstore) DebugPrint() {}

func (qs *Oldstore) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	return &iterator.Null{}, false
}

func (qs *Oldstore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(iterator.Identity)
}

func (qs *Oldstore) Close() error { return nil }

func (qs *Oldstore) QuadDirection(graph.Value, quad.Direction) graph.Value {
	return iterator.Int64Node(0)
}

func (qs *Oldstore) RemoveQuad(t quad.Quad) {}

func (qs *Oldstore) Type() string { return "oldmockstore" }

type Store struct {
	Data []quad.Quad
}

var _ graph.QuadStore = &Store{}

func (qs *Store) ValueOf(s quad.Value) graph.Value {
	return graph.PreFetched(s)
}

func (qs *Store) ApplyDeltas([]graph.Delta, graph.IgnoreOpts) error { return nil }

type quadValue struct {
	q quad.Quad
}

func (q quadValue) Key() interface{} {
	return q.q.String()
}

func (qs *Store) Quad(v graph.Value) quad.Quad { return v.(quadValue).q }

func (qs *Store) NameOf(v graph.Value) quad.Value {
	if v == nil {
		return nil
	}
	return v.(graph.PreFetchedValue).NameOf()
}

func (qs *Store) RemoveQuad(t quad.Quad) {}

func (qs *Store) Type() string { return "mockstore" }

func (qs *Store) QuadDirection(v graph.Value, d quad.Direction) graph.Value {
	return graph.PreFetched(qs.Quad(v).Get(d))
}

func (qs *Store) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	return &iterator.Null{}, false
}

func (qs *Store) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(iterator.Identity)
}

func (qs *Store) Close() error { return nil }

func (qs *Store) Horizon() graph.PrimaryKey { return graph.NewSequentialKey(0) }

func (qs *Store) DebugPrint() {}

func (qs *Store) QuadIterator(d quad.Direction, i graph.Value) graph.Iterator {
	fixed := qs.FixedIterator()
	v := i.(graph.PreFetchedValue).NameOf()
	for _, q := range qs.Data {
		if q.Get(d) == v {
			fixed.Add(quadValue{q})
		}
	}
	return fixed
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
	fixed := qs.FixedIterator()
	for k, _ := range set {
		fixed.Add(graph.PreFetched(quad.Raw(k)))
	}
	return fixed
}

func (qs *Store) QuadsAllIterator() graph.Iterator {
	fixed := qs.FixedIterator()
	for _, q := range qs.Data {
		fixed.Add(quadValue{q})
	}
	return fixed
}

func (qs *Store) Size() int64 { return int64(len(qs.Data)) }
