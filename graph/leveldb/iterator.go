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

type Iterator struct {
	iterator.Base
	nextPrefix     []byte
	checkId        []byte
	dir            graph.Direction
	open           bool
	iter           ldbit.Iterator
	ts             *TripleStore
	ro             *opt.ReadOptions
	originalPrefix string
}

func NewIterator(prefix string, d graph.Direction, value graph.Value, ts *TripleStore) *Iterator {
	var it Iterator
	iterator.BaseInit(&it.Base)
	it.checkId = value.([]byte)
	it.dir = d
	it.originalPrefix = prefix
	it.nextPrefix = make([]byte, 0, 2+ts.hasher.Size())
	it.nextPrefix = append(it.nextPrefix, []byte(prefix)...)
	it.nextPrefix = append(it.nextPrefix, []byte(it.checkId[1:])...)
	it.ro = &opt.ReadOptions{}
	it.ro.DontFillCache = true
	it.iter = ts.db.NewIterator(nil, it.ro)
	it.open = true
	it.ts = ts
	ok := it.iter.Seek(it.nextPrefix)
	if !ok {
		it.open = false
		it.iter.Release()
	}
	return &it
}

func (it *Iterator) Reset() {
	if !it.open {
		it.iter = it.ts.db.NewIterator(nil, it.ro)
		it.open = true
	}
	ok := it.iter.Seek(it.nextPrefix)
	if !ok {
		it.open = false
		it.iter.Release()
	}
}

func (it *Iterator) Clone() graph.Iterator {
	out := NewIterator(it.originalPrefix, it.dir, it.checkId, it.ts)
	out.CopyTagsFrom(it)
	return out
}

func (it *Iterator) Close() {
	if it.open {
		it.iter.Release()
		it.open = false
	}
}

func (it *Iterator) Next() (graph.Value, bool) {
	if it.iter == nil {
		it.Last = nil
		return nil, false
	}
	if !it.open {
		it.Last = nil
		return nil, false
	}
	if !it.iter.Valid() {
		it.Last = nil
		it.Close()
		return nil, false
	}
	if bytes.HasPrefix(it.iter.Key(), it.nextPrefix) {
		out := make([]byte, len(it.iter.Key()))
		copy(out, it.iter.Key())
		it.Last = out
		ok := it.iter.Next()
		if !ok {
			it.Close()
		}
		return out, true
	}
	it.Close()
	it.Last = nil
	return nil, false
}

func PositionOf(prefix []byte, d graph.Direction, ts *TripleStore) int {
	if bytes.Equal(prefix, []byte("sp")) {
		switch d {
		case graph.Subject:
			return 2
		case graph.Predicate:
			return ts.hasher.Size() + 2
		case graph.Object:
			return 2*ts.hasher.Size() + 2
		case graph.Provenance:
			return -1
		}
	}
	if bytes.Equal(prefix, []byte("po")) {
		switch d {
		case graph.Subject:
			return 2*ts.hasher.Size() + 2
		case graph.Predicate:
			return 2
		case graph.Object:
			return ts.hasher.Size() + 2
		case graph.Provenance:
			return -1
		}
	}
	if bytes.Equal(prefix, []byte("os")) {
		switch d {
		case graph.Subject:
			return ts.hasher.Size() + 2
		case graph.Predicate:
			return 2*ts.hasher.Size() + 2
		case graph.Object:
			return 2
		case graph.Provenance:
			return -1
		}
	}
	if bytes.Equal(prefix, []byte("cp")) {
		switch d {
		case graph.Subject:
			return 2*ts.hasher.Size() + 2
		case graph.Predicate:
			return ts.hasher.Size() + 2
		case graph.Object:
			return 3*ts.hasher.Size() + 2
		case graph.Provenance:
			return 2
		}
	}
	panic("unreachable")
}

func (it *Iterator) Check(v graph.Value) bool {
	val := v.([]byte)
	if val[0] == 'z' {
		return false
	}
	offset := PositionOf(val[0:2], it.dir, it.ts)
	if offset != -1 {
		if bytes.HasPrefix(val[offset:], it.checkId[1:]) {
			return true
		}
	} else {
		nameForDir := it.ts.Triple(v).Get(it.dir)
		hashForDir := it.ts.ValueOf(nameForDir).([]byte)
		if bytes.Equal(hashForDir, it.checkId) {
			return true
		}
	}
	return false
}

func (it *Iterator) Size() (int64, bool) {
	return it.ts.SizeOf(it.checkId), true
}

func (it *Iterator) DebugString(indent int) string {
	size, _ := it.Size()
	return fmt.Sprintf("%s(%s %d tags: %v dir: %s size:%d %s)", strings.Repeat(" ", indent), it.Type(), it.UID(), it.Tags(), it.dir, size, it.ts.NameOf(it.checkId))
}

var levelDBType graph.Type

func init() {
	levelDBType = graph.RegisterIterator("leveldb")
}

func Type() graph.Type { return levelDBType }

func (it *Iterator) Type() graph.Type { return levelDBType }
func (it *Iterator) Sorted() bool     { return false }

func (it *Iterator) Optimize() (graph.Iterator, bool) {
	return it, false
}

func (it *Iterator) Stats() graph.IteratorStats {
	s, _ := it.Size()
	return graph.IteratorStats{
		CheckCost: 1,
		NextCost:  2,
		Size:      s,
	}
}
