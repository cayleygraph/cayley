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

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/proto"
)

type QuadIterator struct {
	uid     uint64
	qs      *QuadStore
	ind     QuadIndex
	horizon int64
	vals    []uint64
	size    int64

	tx   BucketTx
	b    Bucket
	it   KVIterator
	done bool

	err  error
	off  int
	ids  []uint64
	buf  []*proto.Primitive
	prim *proto.Primitive
}

var _ graph.Iterator = &QuadIterator{}

func NewQuadIterator(qs *QuadStore, ind QuadIndex, vals []uint64) *QuadIterator {
	return &QuadIterator{
		qs:      qs,
		ind:     ind,
		horizon: qs.horizon(context.TODO()),
		uid:     iterator.NextUID(),
		vals:    vals,
		size:    -1,
	}
}

func (it *QuadIterator) UID() uint64 {
	return it.uid
}

func (it *QuadIterator) Reset() {
	it.off = 0
	it.ids = nil
	it.buf = nil
	it.done = false
	if it.it != nil {
		it.it.Close()
		it.it = it.b.Scan(it.ind.Key(it.vals))
	}
}

func (it *QuadIterator) TagResults(dst map[string]graph.Value) {}

func (it *QuadIterator) Close() error {
	if it.it != nil {
		if err := it.it.Close(); err != nil && it.err == nil {
			it.err = err
		}
		if err := it.tx.Rollback(); err != nil && it.err == nil {
			it.err = err
		}
		it.it = nil
		it.tx = nil
		it.b = nil
	}
	return it.err
}

func (it *QuadIterator) Err() error {
	return it.err
}

func (it *QuadIterator) Result() graph.Value {
	if it.off < 0 || it.prim == nil {
		return nil
	}
	return it.prim
}

func (it *QuadIterator) ensureTx() bool {
	if it.tx != nil {
		return true
	}
	it.tx, it.err = it.qs.db.Tx(false)
	if it.err != nil {
		return false
	}
	it.b = it.tx.Bucket(it.ind.Bucket())
	return true
}

func (it *QuadIterator) Next(ctx context.Context) bool {
	it.prim = nil
	if it.err != nil || it.done {
		return false
	}
	if it.it == nil {
		if !it.ensureTx() {
			return false
		}
		it.it = it.b.Scan(it.ind.Key(it.vals))
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
			it.prim = p
			return true
		}
	}
}

func (it *QuadIterator) NextPath(ctx context.Context) bool {
	return false
}

func (it *QuadIterator) Contains(ctx context.Context, v graph.Value) bool {
	it.prim = nil
	p, ok := v.(*proto.Primitive)
	if !ok {
		return false
	}
	for i, v := range it.vals {
		if p.GetDirection(it.ind.Dirs[i]) != v {
			return false
		}
	}
	return true
}

func (it *QuadIterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *QuadIterator) Size() (int64, bool) {
	if it.err != nil {
		return 0, false
	} else if it.size >= 0 {
		return it.size, true
	}
	ctx := context.TODO()
	if len(it.ind.Dirs) == len(it.vals) {
		var ids []uint64
		it.err = View(it.qs.db, func(tx BucketTx) error {
			b := tx.Bucket(it.ind.Bucket())
			vals, err := b.Get(ctx, [][]byte{it.ind.Key(it.vals)})
			if err != nil {
				return err
			}
			ids, err = decodeIndex(vals[0])
			if err != nil {
				return err
			}
			return nil
		})
		if it.err != nil {
			return 0, false
		}
		it.size = int64(len(ids))
		return it.size, true
	}
	return 1 + it.qs.Size()/2, false
}

func (it *QuadIterator) String() string {
	return fmt.Sprintf("KVQuads(%v)", it.ind)
}

func (it *QuadIterator) Sorted() bool { return true }

func (it *QuadIterator) Optimize() (graph.Iterator, bool) {
	return it, false
}

func (it *QuadIterator) Stats() graph.IteratorStats {
	s, exact := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     2,
		Size:         s,
		ExactSize:    exact,
	}
}
