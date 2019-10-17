// Copyright 2016 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"context"
	"fmt"

	"github.com/hidal-go/hidalgo/kv"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/proto"
	"github.com/cayleygraph/cayley/graph/refs"
)

type QuadIterator struct {
	qs   *QuadStore
	ind  QuadIndex
	vals []uint64

	size refs.Size
	err  error
}

func (qs *QuadStore) newQuadIterator(ind QuadIndex, vals []uint64) *QuadIterator {
	return &QuadIterator{
		qs:   qs,
		ind:  ind,
		vals: vals,
		size: refs.Size{Value: -1},
	}
}

func (it *QuadIterator) Iterate() iterator.Scanner {
	return it.qs.newQuadIteratorNext(it.ind, it.vals)
}

func (it *QuadIterator) Lookup() iterator.Index {
	return it.qs.newQuadIteratorContains(it.ind, it.vals)
}

func (it *QuadIterator) SubIterators() []iterator.Shape {
	return nil
}

func (it *QuadIterator) getSize(ctx context.Context) (refs.Size, error) {
	if it.err != nil {
		return refs.Size{}, it.err
	} else if it.size.Value >= 0 {
		return it.size, nil
	}
	if len(it.ind.Dirs) == len(it.vals) {
		sz, err := it.qs.indexSize(ctx, it.ind, it.vals)
		if err != nil {
			it.err = err
			return refs.Size{}, it.err
		}
		it.size = sz
		return sz, nil
	}
	sz := refs.Size{Value: 1 + it.qs.Size()/2, Exact: false}
	it.size = sz
	return sz, nil
}

func (it *QuadIterator) String() string {
	return fmt.Sprintf("KVQuads(%v)", it.ind)
}

func (it *QuadIterator) Sorted() bool { return true }

func (it *QuadIterator) Optimize(ctx context.Context) (iterator.Shape, bool) {
	return it, false
}

func (it *QuadIterator) Stats(ctx context.Context) (iterator.Costs, error) {
	s, err := it.getSize(ctx)
	return iterator.Costs{
		ContainsCost: 1,
		NextCost:     2,
		Size:         s,
	}, err
}

type quadIteratorNext struct {
	qs   *QuadStore
	ind  QuadIndex
	vals []uint64

	tx   kv.Tx
	it   kv.Iterator
	done bool

	err  error
	off  int
	ids  []uint64
	buf  []*proto.Primitive
	prim *proto.Primitive
}

func (qs *QuadStore) newQuadIteratorNext(ind QuadIndex, vals []uint64) *quadIteratorNext {
	return &quadIteratorNext{
		qs:   qs,
		ind:  ind,
		vals: vals,
	}
}

func (it *quadIteratorNext) TagResults(dst map[string]graph.Ref) {}

func (it *quadIteratorNext) Close() error {
	if it.it != nil {
		if err := it.it.Close(); err != nil && it.err == nil {
			it.err = err
		}
		if err := it.tx.Close(); err != nil && it.err == nil {
			it.err = err
		}
		it.it = nil
		it.tx = nil
	}
	return it.err
}

func (it *quadIteratorNext) Err() error {
	return it.err
}

func (it *quadIteratorNext) Result() graph.Ref {
	if it.off < 0 || it.prim == nil {
		return nil
	}
	return it.prim
}

func (it *quadIteratorNext) ensureTx() bool {
	if it.tx != nil {
		return true
	}
	it.tx, it.err = it.qs.db.Tx(false)
	if it.err != nil {
		return false
	}
	it.tx = wrapTx(it.tx)
	return true
}

func (it *quadIteratorNext) Next(ctx context.Context) bool {
	it.prim = nil
	if it.err != nil || it.done {
		return false
	}
	if it.it == nil {
		if !it.ensureTx() {
			return false
		}
		it.it = it.tx.Scan(it.ind.Key(it.vals))
		if err := it.Err(); err != nil {
			it.err = err
			return false
		}
	}
	for {
		if len(it.buf) == 0 {
			for len(it.ids[it.off:]) == 0 {
				it.off = 0
				it.ids = nil
				it.buf = nil
				if !it.it.Next(ctx) {
					it.Close()
					it.done = true
					return false
				}
				it.ids, it.err = decodeIndex(it.it.Val())
				if it.err != nil {
					return false
				}
			}
			ids := it.ids[it.off:]
			if len(ids) > nextBatch {
				ids = ids[:nextBatch]
			}
			it.buf, it.err = it.qs.getPrimitivesFromLog(ctx, it.tx, ids)
			if it.err != nil {
				return false
			}
		} else {
			it.buf, it.off = it.buf[1:], it.off+1
		}
		for ; len(it.buf) > 0; it.buf, it.off = it.buf[1:], it.off+1 {
			p := it.buf[0]
			if p == nil || p.Deleted {
				continue
			}
			// TODO(dennwc): shouldn't this check the horizon?
			it.prim = p
			return true
		}
	}
}

func (it *quadIteratorNext) NextPath(ctx context.Context) bool {
	return false
}

func (it *quadIteratorNext) String() string {
	return fmt.Sprintf("KVQuadsNext(%v)", it.ind)
}

func (it *quadIteratorNext) Sorted() bool { return true }

type quadIteratorContains struct {
	qs   *QuadStore
	ind  QuadIndex
	vals []uint64

	err  error
	prim *proto.Primitive
}

func (qs *QuadStore) newQuadIteratorContains(ind QuadIndex, vals []uint64) *quadIteratorContains {
	return &quadIteratorContains{
		qs:   qs,
		ind:  ind,
		vals: vals,
	}
}

func (it *quadIteratorContains) TagResults(dst map[string]graph.Ref) {}

func (it *quadIteratorContains) Close() error {
	return it.err
}

func (it *quadIteratorContains) Err() error {
	return it.err
}

func (it *quadIteratorContains) Result() graph.Ref {
	if it.prim == nil {
		return nil
	}
	return it.prim
}

func (it *quadIteratorContains) NextPath(ctx context.Context) bool {
	return false
}

func (it *quadIteratorContains) Contains(ctx context.Context, v graph.Ref) bool {
	it.prim = nil
	// TODO(dennwc): shouldn't this check the horizon?
	p, ok := v.(*proto.Primitive)
	if !ok {
		return false
	}
	for i, v := range it.vals {
		if p.GetDirection(it.ind.Dirs[i]) != v {
			return false
		}
	}
	it.prim = p
	return true
}

func (it *quadIteratorContains) String() string {
	return fmt.Sprintf("KVQuadsContains(%v)", it.ind)
}

func (it *quadIteratorContains) Sorted() bool { return true }
