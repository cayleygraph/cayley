package kv

import (
	"context"
	"fmt"

	"github.com/hidal-go/hidalgo/kv"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/shape"
	"github.com/cayleygraph/quad"
)

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator(true, qs, nil)
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return NewAllIterator(false, qs, nil)
}

func (qs *QuadStore) indexSize(ctx context.Context, ind QuadIndex, vals []uint64) (graph.Size, error) {
	var sz int64
	err := kv.View(qs.db, func(tx kv.Tx) error {
		val, err := tx.Get(ctx, ind.Key(vals))
		if err != nil {
			return err
		}
		sz, err = countIndex(val)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return graph.Size{}, err
	}
	if len(ind.Dirs) == len(vals) {
		return graph.Size{
			Size:  sz,
			Exact: true,
		}, nil
	}
	return graph.Size{
		Size:  1 + sz/2,
		Exact: false,
	}, nil
}

func (qs *QuadStore) QuadIteratorSize(ctx context.Context, d quad.Direction, v graph.Ref) (graph.Size, error) {
	vi, ok := v.(Int64Value)
	if !ok {
		return graph.Size{Size: 0, Exact: true}, nil
	}
	qs.indexes.RLock()
	all := qs.indexes.all
	qs.indexes.RUnlock()
	for _, ind := range all {
		if len(ind.Dirs) == 1 && ind.Dirs[0] == d {
			return qs.indexSize(ctx, ind, []uint64{uint64(vi)})
		}
	}
	st, err := qs.Stats(ctx, false)
	if err != nil {
		return graph.Size{}, err
	}
	return graph.Size{
		Size:  1 + st.Quads.Size/2,
		Exact: false,
	}, nil
}

func (qs *QuadStore) QuadIterator(dir quad.Direction, v graph.Ref) graph.Iterator {
	if v == nil {
		return iterator.NewNull()
	}
	vi, ok := v.(Int64Value)
	if !ok {
		return iterator.NewError(fmt.Errorf("unexpected node type: %T", v))
	}
	// Find the best index for this direction.
	if ind := qs.bestIndexes([]quad.Direction{dir}); len(ind) == 1 {
		// this will scan the prefix automatically
		return NewQuadIterator(qs, ind[0], []uint64{uint64(vi)})
	}
	// Fallback: iterate all quads and check the corresponding direction.
	return NewAllIterator(false, qs, &constraint{
		dir: dir,
		val: vi,
	})
}

func (qs *QuadStore) OptimizeShape(s shape.Shape) (shape.Shape, bool) {
	switch s := s.(type) {
	case shape.QuadsAction:
		return qs.optimizeQuadsAction(s)
	}
	return s, false
}

func (qs *QuadStore) optimizeQuadsAction(s shape.QuadsAction) (shape.Shape, bool) {
	if len(s.Filter) == 0 {
		return s, false
	}
	dirs := make([]quad.Direction, 0, len(s.Filter))
	for d := range s.Filter {
		dirs = append(dirs, d)
	}
	ind := qs.bestIndexes(dirs)
	if len(ind) != 1 {
		return s, false // TODO(dennwc): allow intersecting indexes
	}
	quads := IndexScan{Index: ind[0]}
	for _, d := range ind[0].Dirs {
		v, ok := s.Filter[d].(Int64Value)
		if !ok {
			return s, false
		}
		quads.Values = append(quads.Values, uint64(v))
	}
	return s.SimplifyFrom(quads), true
}

type IndexScan struct {
	Index  QuadIndex
	Values []uint64
}

func (s IndexScan) BuildIterator(qs graph.QuadStore) graph.Iterator {
	kqs, ok := qs.(*QuadStore)
	if !ok {
		return iterator.NewError(fmt.Errorf("expected KV quadstore, got: %T", qs))
	}
	return NewQuadIterator(kqs, s.Index, s.Values)
}

func (s IndexScan) Optimize(r shape.Optimizer) (shape.Shape, bool) {
	return s, false
}
