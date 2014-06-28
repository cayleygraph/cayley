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

func NewLevelDBAllIterator(prefix, dir string, ts *TripleStore) *AllIterator {
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

func (a *AllIterator) Reset() {
	if !a.open {
		a.it = a.ts.db.NewIterator(nil, a.ro)
		a.open = true
	}
	a.it.Seek(a.prefix)
	if !a.it.Valid() {
		a.open = false
		a.it.Release()
	}
}

func (a *AllIterator) Clone() graph.Iterator {
	out := NewLevelDBAllIterator(string(a.prefix), a.dir, a.ts)
	out.CopyTagsFrom(a)
	return out
}

func (a *AllIterator) Next() (graph.TSVal, bool) {
	if !a.open {
		a.Last = nil
		return nil, false
	}
	var out []byte
	out = make([]byte, len(a.it.Key()))
	copy(out, a.it.Key())
	a.it.Next()
	if !a.it.Valid() {
		a.Close()
	}
	if !bytes.HasPrefix(out, a.prefix) {
		a.Close()
		return nil, false
	}
	a.Last = out
	return out, true
}

func (a *AllIterator) Check(v graph.TSVal) bool {
	a.Last = v
	return true
}

func (lit *AllIterator) Close() {
	if lit.open {
		lit.it.Release()
		lit.open = false
	}
}

func (a *AllIterator) Size() (int64, bool) {
	size, err := a.ts.GetApproximateSizeForPrefix(a.prefix)
	if err == nil {
		return size, false
	}
	// INT64_MAX
	return int64(^uint64(0) >> 1), false
}

func (lit *AllIterator) DebugString(indent int) string {
	size, _ := lit.Size()
	return fmt.Sprintf("%s(%s tags: %v leveldb size:%d %s %p)", strings.Repeat(" ", indent), lit.Type(), lit.Tags(), size, lit.dir, lit)
}

func (lit *AllIterator) Type() string { return "all" }
func (lit *AllIterator) Sorted() bool { return false }

func (lit *AllIterator) Optimize() (graph.Iterator, bool) {
	return lit, false
}

func (lit *AllIterator) GetStats() *graph.IteratorStats {
	s, _ := lit.Size()
	return &graph.IteratorStats{
		CheckCost: 1,
		NextCost:  2,
		Size:      s,
	}
}
