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
	"fmt"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/proto"
	"github.com/cayleygraph/cayley/quad"
)

type QuadIterator struct {
	qs      *QuadStore
	ind     QuadIndex
	err     error
	uid     uint64
	tags    graph.Tagger
	horizon int64
	off     int
	ids     []uint64
	buf     []*proto.Primitive
	v       Int64Value
	dir     quad.Direction
	prim    *proto.Primitive
}

var _ graph.Iterator = &QuadIterator{}

func NewQuadIterator(ind QuadIndex, dir quad.Direction, v Int64Value, qs *QuadStore) *QuadIterator {
	return &QuadIterator{
		qs:      qs,
		ind:     ind,
		horizon: qs.horizon(),
		uid:     iterator.NextUID(),
		v:       v,
		dir:     dir,
	}
}

func (it *QuadIterator) UID() uint64 {
	return it.uid
}

func (it *QuadIterator) Reset() {
	it.off = 0
}

func (it *QuadIterator) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *QuadIterator) TagResults(dst map[string]graph.Value) {
	it.tags.TagResult(dst, it.Result())
}

func (it *QuadIterator) Clone() graph.Iterator {
	out := NewQuadIterator(it.ind, it.dir, it.v, it.qs)
	out.tags.CopyFrom(it)
	out.ids = it.ids
	out.horizon = it.horizon
	return out
}

func (it *QuadIterator) Close() error {
	return nil
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

func (it *QuadIterator) Next() bool {
	it.prim = nil
	if it.err != nil {
		return false
	}
	it.ensureIDs()
	for {
		if len(it.buf) == 0 {
			if it.off >= len(it.ids) {
				return false
			}
			ids := it.ids[it.off:]
			if len(ids) > nextBatch {
				ids = ids[:nextBatch]
			}
			if len(ids) == 0 {
				return false
			}
			it.buf, it.err = it.qs.getPrimitives(ids)
			if it.err != nil || len(ids) == 0 {
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

func (it *QuadIterator) NextPath() bool {
	return false
}

func (it *QuadIterator) Contains(v graph.Value) bool {
	it.prim = nil
	p := v.(*proto.Primitive)
	if p.GetDirection(it.dir) == uint64(it.v) {
		it.prim = p
		return true
	}
	return false
}

func (it *QuadIterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *QuadIterator) Size() (int64, bool) {
	it.ensureIDs()
	return int64(len(it.ids)), true
}

func (it *QuadIterator) ensureIDs() {
	if it.ids != nil {
		return
	}
	err := View(it.qs.db, func(tx BucketTx) error {
		inds, err := it.qs.getBucketIndexes(tx, []BucketKey{
			{
				Bucket: it.ind.Bucket(),
				Key:    it.ind.Key([]uint64{uint64(it.v)}),
			},
		})
		if err != nil {
			return err
		}
		it.ids = inds[0]
		return nil
	})
	if err != nil {
		it.ids = make([]uint64, 0)
		it.err = err
	}
}

func (it *QuadIterator) Describe() graph.Description {
	size, _ := it.Size()
	return graph.Description{
		UID:       it.UID(),
		Type:      it.Type(),
		Tags:      it.tags.Tags(),
		Size:      size,
		Direction: it.dir,
		Name:      fmt.Sprint(it.v, it.ids),
	}
}

func (it *QuadIterator) Type() graph.Type { return "kv_quad" }
func (it *QuadIterator) Sorted() bool     { return true }

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
