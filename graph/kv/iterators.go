package kv

import (
	"context"
	"fmt"

	"github.com/hidal-go/hidalgo/kv"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
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

	qs.indexes.RLock()
	all := qs.indexes.all
	qs.indexes.RUnlock()
	for _, ind := range all {
		if len(ind.Dirs) == 1 && ind.Dirs[0] == dir {
			return NewQuadIterator(qs, ind, []uint64{uint64(vi)})
		}
	}
	return NewAllIterator(false, qs, &constraint{
		dir: dir,
		val: vi,
	})
}
