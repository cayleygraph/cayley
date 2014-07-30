// Copyright 2014 The Cayley Authors. All rights reserved.
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

package leveldb

import (
	"bytes"
	"fmt"
	"strings"

	ldbit "github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
)

type AllIterator struct {
	iterator.Base
	uid    uint64
	tags   graph.Tagger
	prefix []byte
	dir    graph.Direction
	open   bool
	iter   ldbit.Iterator
	ts     *TripleStore
	ro     *opt.ReadOptions
}

func NewAllIterator(prefix string, d graph.Direction, ts *TripleStore) *AllIterator {
	opts := &opt.ReadOptions{
		DontFillCache: true,
	}

	it := AllIterator{
		uid:    iterator.NextUID(),
		ro:     opts,
		iter:   ts.db.NewIterator(nil, opts),
		prefix: []byte(prefix),
		dir:    d,
		open:   true,
		ts:     ts,
	}
	iterator.BaseInit(&it.Base)

	it.iter.Seek(it.prefix)
	if !it.iter.Valid() {
		// FIXME(kortschak) What are the semantics here? Is this iterator usable?
		// If not, we should return nil *Iterator and an error.
		it.open = false
		it.iter.Release()
	}

	return &it
}

func (it *AllIterator) UID() uint64 {
	return it.uid
}

func (it *AllIterator) Reset() {
	if !it.open {
		it.iter = it.ts.db.NewIterator(nil, it.ro)
		it.open = true
	}
	it.iter.Seek(it.prefix)
	if !it.iter.Valid() {
		it.open = false
		it.iter.Release()
	}
}

func (it *AllIterator) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *AllIterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *AllIterator) Clone() graph.Iterator {
	out := NewAllIterator(string(it.prefix), it.dir, it.ts)
	out.tags.CopyFrom(it)
	return out
}

func (it *AllIterator) Next() (graph.Value, bool) {
	if !it.open {
		it.Last = nil
		return nil, false
	}
	var out []byte
	out = make([]byte, len(it.iter.Key()))
	copy(out, it.iter.Key())
	it.iter.Next()
	if !it.iter.Valid() {
		it.Close()
	}
	if !bytes.HasPrefix(out, it.prefix) {
		it.Close()
		return nil, false
	}
	it.Last = out
	return out, true
}

// No subiterators.
func (it *AllIterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *AllIterator) Check(v graph.Value) bool {
	it.Last = v
	return true
}

func (it *AllIterator) Close() {
	if it.open {
		it.iter.Release()
		it.open = false
	}
}

func (it *AllIterator) Size() (int64, bool) {
	size, err := it.ts.SizeOfPrefix(it.prefix)
	if err == nil {
		return size, false
	}
	// INT64_MAX
	return int64(^uint64(0) >> 1), false
}

func (it *AllIterator) DebugString(indent int) string {
	size, _ := it.Size()
	return fmt.Sprintf("%s(%s tags: %v leveldb size:%d %s %p)", strings.Repeat(" ", indent), it.Type(), it.tags.Tags(), size, it.dir, it)
}

func (it *AllIterator) Type() graph.Type { return graph.All }
func (it *AllIterator) Sorted() bool     { return false }

func (it *AllIterator) Optimize() (graph.Iterator, bool) {
	return it, false
}

func (it *AllIterator) Stats() graph.IteratorStats {
	s, _ := it.Size()
	return graph.IteratorStats{
		CheckCost: 1,
		NextCost:  2,
		Size:      s,
	}
}
