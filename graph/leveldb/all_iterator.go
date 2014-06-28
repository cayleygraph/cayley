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

	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/google/cayley/graph"
)

type AllIterator struct {
	graph.BaseIterator
	prefix []byte
	dir    string
	open   bool
	it     iterator.Iterator
	ts     *TripleStore
	ro     *opt.ReadOptions
}

func NewAllIterator(prefix, dir string, ts *TripleStore) *AllIterator {
	var it AllIterator
	graph.BaseIteratorInit(&it.BaseIterator)
	it.ro = &opt.ReadOptions{}
	it.ro.DontFillCache = true
	it.it = ts.db.NewIterator(nil, it.ro)
	it.prefix = []byte(prefix)
	it.dir = dir
	it.open = true
	it.ts = ts
	it.it.Seek(it.prefix)
	if !it.it.Valid() {
		it.open = false
		it.it.Release()
	}
	return &it
}

func (it *AllIterator) Reset() {
	if !it.open {
		it.it = it.ts.db.NewIterator(nil, it.ro)
		it.open = true
	}
	it.it.Seek(it.prefix)
	if !it.it.Valid() {
		it.open = false
		it.it.Release()
	}
}

func (it *AllIterator) Clone() graph.Iterator {
	out := NewAllIterator(string(it.prefix), it.dir, it.ts)
	out.CopyTagsFrom(it)
	return out
}

func (it *AllIterator) Next() (graph.TSVal, bool) {
	if !it.open {
		it.Last = nil
		return nil, false
	}
	var out []byte
	out = make([]byte, len(it.it.Key()))
	copy(out, it.it.Key())
	it.it.Next()
	if !it.it.Valid() {
		it.Close()
	}
	if !bytes.HasPrefix(out, it.prefix) {
		it.Close()
		return nil, false
	}
	it.Last = out
	return out, true
}

func (it *AllIterator) Check(v graph.TSVal) bool {
	it.Last = v
	return true
}

func (lit *AllIterator) Close() {
	if lit.open {
		lit.it.Release()
		lit.open = false
	}
}

func (it *AllIterator) Size() (int64, bool) {
	size, err := it.ts.GetApproximateSizeForPrefix(it.prefix)
	if err == nil {
		return size, false
	}
	// INT64_MAX
	return int64(^uint64(0) >> 1), false
}

func (it *AllIterator) DebugString(indent int) string {
	size, _ := it.Size()
	return fmt.Sprintf("%s(%s tags: %v leveldb size:%d %s %p)", strings.Repeat(" ", indent), it.Type(), it.Tags(), size, it.dir, it)
}

func (it *AllIterator) Type() string { return "all" }
func (it *AllIterator) Sorted() bool { return false }

func (it *AllIterator) Optimize() (graph.Iterator, bool) {
	return it, false
}

func (it *AllIterator) GetStats() *graph.IteratorStats {
	s, _ := it.Size()
	return &graph.IteratorStats{
		CheckCost: 1,
		NextCost:  2,
		Size:      s,
	}
}
