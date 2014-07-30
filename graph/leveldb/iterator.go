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
	uid            uint64
	tags           graph.Tagger
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
	vb := value.([]byte)
	p := make([]byte, 0, 2+ts.hasher.Size())
	p = append(p, []byte(prefix)...)
	p = append(p, []byte(vb[1:])...)

	opts := &opt.ReadOptions{
		DontFillCache: true,
	}

	it := Iterator{
		uid:            iterator.NextUID(),
		nextPrefix:     p,
		checkId:        vb,
		dir:            d,
		originalPrefix: prefix,
		ro:             opts,
		iter:           ts.db.NewIterator(nil, opts),
		open:           true,
		ts:             ts,
	}
	iterator.BaseInit(&it.Base)

	ok := it.iter.Seek(it.nextPrefix)
	if !ok {
		// FIXME(kortschak) What are the semantics here? Is this iterator usable?
		// If not, we should return nil *Iterator and an error.
		it.open = false
		it.iter.Release()
	}

	return &it
}

func (it *Iterator) UID() uint64 {
	return it.uid
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

func (it *Iterator) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *Iterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *Iterator) Clone() graph.Iterator {
	out := NewIterator(it.originalPrefix, it.dir, it.checkId, it.ts)
	out.tags.CopyFrom(it)
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
	return fmt.Sprintf("%s(%s %d tags: %v dir: %s size:%d %s)", strings.Repeat(" ", indent), it.Type(), it.UID(), it.tags.Tags(), it.dir, size, it.ts.NameOf(it.checkId))
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
