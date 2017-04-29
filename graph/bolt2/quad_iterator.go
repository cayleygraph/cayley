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

package bolt2

import (
	"fmt"
	"sort"

	"github.com/boltdb/bolt"
	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
)

var (
	boltType graph.Type
)

func init() {
	boltType = graph.RegisterIterator("bolt_quad")
}

type QuadIterator struct {
	qs      *QuadStore
	err     error
	uid     uint64
	tags    graph.Tagger
	horizon int64
	off     int
	ids     []uint64
	v       Int64Value
	dir     quad.Direction
}

var _ graph.Iterator = &QuadIterator{}

func Type() graph.Type { return boltType }

func NewQuadIterator(dir quad.Direction, v Int64Value, qs *QuadStore) *QuadIterator {
	return &QuadIterator{
		qs:      qs,
		horizon: qs.horizon,
		uid:     iterator.NextUID(),
		v:       v,
		off:     -1,
		dir:     dir,
	}
}

func (it *QuadIterator) UID() uint64 {
	return it.uid
}

func (it *QuadIterator) Reset() {
	it.off = -1
}

func (it *QuadIterator) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *QuadIterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *QuadIterator) Clone() graph.Iterator {
	out := NewQuadIterator(it.dir, it.v, it.qs)
	out.tags.CopyFrom(it)
	out.ids = it.ids
	return out
}

func (it *QuadIterator) Close() error {
	return nil
}

func (it *QuadIterator) Err() error {
	return it.err
}

func (it *QuadIterator) Result() graph.Value {
	if it.off == -1 {
		return nil
	}
	return Int64Value(it.ids[it.off])
}

func (it *QuadIterator) Next() bool {
	it.ensureIDs()
	it.off++
	if it.off >= len(it.ids) {
		return false
	}
	return true
}

func (it *QuadIterator) NextPath() bool {
	return false
}

func (it *QuadIterator) Contains(v graph.Value) bool {
	it.ensureIDs()
	x := uint64(v.(Int64Value))
	n := sort.Search(len(it.ids), func(i int) bool { return it.ids[i] >= x })
	if n < len(it.ids) && x == it.ids[n] {
		it.off = n
		return true
	}
	it.off = -1
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
	err := it.qs.db.View(func(tx *bolt.Tx) error {
		b := subjectIndex
		if it.dir == quad.Object {
			b = objectIndex
		}
		var err error
		it.ids, err = it.qs.getBucketIndex(tx, b, uint64(it.v))
		return err
	})
	if err != nil {
		it.ids = make([]uint64, 0)
		clog.Errorf("error getting index for iterator: %s", err)
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

func (it *QuadIterator) Type() graph.Type { return boltType }
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
