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

type Iterator struct {
	graph.BaseIterator
	nextPrefix     []byte
	checkId        []byte
	dir            string
	open           bool
	dbIt           iterator.Iterator
	ts             *TripleStore
	ro             *opt.ReadOptions
	originalPrefix string
}

func NewIterator(prefix, dir string, value graph.TSVal, ts *TripleStore) *Iterator {
	var it Iterator
	graph.BaseIteratorInit(&it.BaseIterator)
	it.checkId = value.([]byte)
	it.dir = dir
	it.originalPrefix = prefix
	it.nextPrefix = make([]byte, 0, 2+ts.hasher.Size())
	it.nextPrefix = append(it.nextPrefix, []byte(prefix)...)
	it.nextPrefix = append(it.nextPrefix, []byte(it.checkId[1:])...)
	it.ro = &opt.ReadOptions{}
	it.ro.DontFillCache = true
	it.dbIt = ts.db.NewIterator(nil, it.ro)
	it.open = true
	it.ts = ts
	ok := it.dbIt.Seek(it.nextPrefix)
	if !ok {
		it.open = false
		it.dbIt.Release()
	}
	return &it
}

func (it *Iterator) Reset() {
	if !it.open {
		it.dbIt = it.ts.db.NewIterator(nil, it.ro)
		it.open = true
	}
	ok := it.dbIt.Seek(it.nextPrefix)
	if !ok {
		it.open = false
		it.dbIt.Release()
	}
}

func (it *Iterator) Clone() graph.Iterator {
	out := NewIterator(it.originalPrefix, it.dir, it.checkId, it.ts)
	out.CopyTagsFrom(it)
	return out
}

func (it *Iterator) Close() {
	if it.open {
		it.dbIt.Release()
		it.open = false
	}
}

func (it *Iterator) Next() (graph.TSVal, bool) {
	if it.dbIt == nil {
		it.Last = nil
		return nil, false
	}
	if !it.open {
		it.Last = nil
		return nil, false
	}
	if !it.dbIt.Valid() {
		it.Last = nil
		it.Close()
		return nil, false
	}
	if bytes.HasPrefix(it.dbIt.Key(), it.nextPrefix) {
		out := make([]byte, len(it.dbIt.Key()))
		copy(out, it.dbIt.Key())
		it.Last = out
		ok := it.dbIt.Next()
		if !ok {
			it.Close()
		}
		return out, true
	}
	it.Close()
	it.Last = nil
	return nil, false
}

func GetPositionFromPrefix(prefix []byte, dir string, ts *TripleStore) int {
	if bytes.Equal(prefix, []byte("sp")) {
		switch dir {
		case "s":
			return 2
		case "p":
			return ts.hasher.Size() + 2
		case "o":
			return 2*ts.hasher.Size() + 2
		case "c":
			return -1
		}
	}
	if bytes.Equal(prefix, []byte("po")) {
		switch dir {
		case "s":
			return 2*ts.hasher.Size() + 2
		case "p":
			return 2
		case "o":
			return ts.hasher.Size() + 2
		case "c":
			return -1
		}
	}
	if bytes.Equal(prefix, []byte("os")) {
		switch dir {
		case "s":
			return ts.hasher.Size() + 2
		case "p":
			return 2*ts.hasher.Size() + 2
		case "o":
			return 2
		case "c":
			return -1
		}
	}
	if bytes.Equal(prefix, []byte("cp")) {
		switch dir {
		case "s":
			return 2*ts.hasher.Size() + 2
		case "p":
			return ts.hasher.Size() + 2
		case "o":
			return 3*ts.hasher.Size() + 2
		case "c":
			return 2
		}
	}
	panic("Notreached")
}

func (it *Iterator) Check(v graph.TSVal) bool {
	val := v.([]byte)
	if val[0] == 'z' {
		return false
	}
	offset := GetPositionFromPrefix(val[0:2], it.dir, it.ts)
	if offset != -1 {
		if bytes.HasPrefix(val[offset:], it.checkId[1:]) {
			return true
		}
	} else {
		nameForDir := it.ts.GetTriple(v).Get(it.dir)
		hashForDir := it.ts.GetIdFor(nameForDir).([]byte)
		if bytes.Equal(hashForDir, it.checkId) {
			return true
		}
	}
	return false
}

func (it *Iterator) Size() (int64, bool) {
	return it.ts.GetSizeFor(it.checkId), true
}

func (it *Iterator) DebugString(indent int) string {
	size, _ := it.Size()
	return fmt.Sprintf("%s(%s %d tags: %v dir: %s size:%d %s)", strings.Repeat(" ", indent), it.Type(), it.GetUid(), it.Tags(), it.dir, size, it.ts.GetNameFor(it.checkId))
}

func (it *Iterator) Type() string { return "leveldb" }
func (it *Iterator) Sorted() bool { return false }

func (it *Iterator) Optimize() (graph.Iterator, bool) {
	return it, false
}

func (it *Iterator) GetStats() *graph.IteratorStats {
	s, _ := it.Size()
	return &graph.IteratorStats{
		CheckCost: 1,
		NextCost:  2,
		Size:      s,
	}
}
