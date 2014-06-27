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
	_ "encoding/binary"
	"fmt"
	"github.com/google/cayley/src/graph"
	leveldb_it "github.com/syndtr/goleveldb/leveldb/iterator"
	leveldb_opt "github.com/syndtr/goleveldb/leveldb/opt"
	"strings"
)

type LevelDBIterator struct {
	graph.BaseIterator
	nextPrefix     []byte
	checkId        []byte
	dir            string
	open           bool
	it             leveldb_it.Iterator
	ts             *LevelDBTripleStore
	ro             *leveldb_opt.ReadOptions
	originalPrefix string
}

func NewLevelDBIterator(prefix, dir string, value graph.TSVal, ts *LevelDBTripleStore) *LevelDBIterator {
	var it LevelDBIterator
	graph.BaseIteratorInit(&it.BaseIterator)
	it.checkId = value.([]byte)
	it.dir = dir
	it.originalPrefix = prefix
	it.nextPrefix = make([]byte, 0, 2+ts.hasher.Size())
	it.nextPrefix = append(it.nextPrefix, []byte(prefix)...)
	it.nextPrefix = append(it.nextPrefix, []byte(it.checkId[1:])...)
	it.ro = &leveldb_opt.ReadOptions{}
	it.ro.DontFillCache = true
	it.it = ts.db.NewIterator(nil, it.ro)
	it.open = true
	it.ts = ts
	ok := it.it.Seek(it.nextPrefix)
	if !ok {
		it.open = false
		it.it.Release()
	}
	return &it
}

func (lit *LevelDBIterator) Reset() {
	if !lit.open {
		lit.it = lit.ts.db.NewIterator(nil, lit.ro)
		lit.open = true
	}
	ok := lit.it.Seek(lit.nextPrefix)
	if !ok {
		lit.open = false
		lit.it.Release()
	}
}

func (lit *LevelDBIterator) Clone() graph.Iterator {
	out := NewLevelDBIterator(lit.originalPrefix, lit.dir, lit.checkId, lit.ts)
	out.CopyTagsFrom(lit)
	return out
}

func (lit *LevelDBIterator) Close() {
	if lit.open {
		lit.it.Release()
		lit.open = false
	}
}

func (lit *LevelDBIterator) Next() (graph.TSVal, bool) {
	if lit.it == nil {
		lit.Last = nil
		return nil, false
	}
	if !lit.open {
		lit.Last = nil
		return nil, false
	}
	if !lit.it.Valid() {
		lit.Last = nil
		lit.Close()
		return nil, false
	}
	if bytes.HasPrefix(lit.it.Key(), lit.nextPrefix) {
		out := make([]byte, len(lit.it.Key()))
		copy(out, lit.it.Key())
		lit.Last = out
		ok := lit.it.Next()
		if !ok {
			lit.Close()
		}
		return out, true
	}
	lit.Close()
	lit.Last = nil
	return nil, false
}

func GetPositionFromPrefix(prefix []byte, dir string, ts *LevelDBTripleStore) int {
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

func (lit *LevelDBIterator) Check(v graph.TSVal) bool {
	val := v.([]byte)
	if val[0] == 'z' {
		return false
	}
	offset := GetPositionFromPrefix(val[0:2], lit.dir, lit.ts)
	if offset != -1 {
		if bytes.HasPrefix(val[offset:], lit.checkId[1:]) {
			return true
		}
	} else {
		nameForDir := lit.ts.GetTriple(v).Get(lit.dir)
		hashForDir := lit.ts.GetIdFor(nameForDir).([]byte)
		if bytes.Equal(hashForDir, lit.checkId) {
			return true
		}
	}
	return false
}

func (lit *LevelDBIterator) Size() (int64, bool) {
	return lit.ts.GetSizeFor(lit.checkId), true
}

func (lit *LevelDBIterator) DebugString(indent int) string {
	size, _ := lit.Size()
	return fmt.Sprintf("%s(%s %d tags: %v dir: %s size:%d %s)", strings.Repeat(" ", indent), lit.Type(), lit.GetUid(), lit.Tags(), lit.dir, size, lit.ts.GetNameFor(lit.checkId))
}

func (lit *LevelDBIterator) Type() string { return "leveldb" }
func (lit *LevelDBIterator) Sorted() bool { return false }

func (lit *LevelDBIterator) Optimize() (graph.Iterator, bool) {
	return lit, false
}

func (lit *LevelDBIterator) GetStats() *graph.IteratorStats {
	s, _ := lit.Size()
	return &graph.IteratorStats{
		CheckCost: 1,
		NextCost:  2,
		Size:      s,
	}
}
