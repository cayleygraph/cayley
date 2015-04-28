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
	"encoding/json"

	"github.com/barakmich/glog"
	ldbit "github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

type Iterator struct {
	uid            uint64
	tags           graph.Tagger
	nextPrefix     []byte
	checkID        []byte
	dir            quad.Direction
	open           bool
	iter           ldbit.Iterator
	qs             *QuadStore
	ro             *opt.ReadOptions
	originalPrefix string
	result         graph.Value
}

func NewIterator(prefix string, d quad.Direction, value graph.Value, qs *QuadStore) *Iterator {
	vb := value.(Token)
	p := make([]byte, 0, 2+hashSize)
	p = append(p, []byte(prefix)...)
	p = append(p, []byte(vb[1:])...)

	opts := &opt.ReadOptions{
		DontFillCache: true,
	}

	it := Iterator{
		uid:            iterator.NextUID(),
		nextPrefix:     p,
		checkID:        vb,
		dir:            d,
		originalPrefix: prefix,
		ro:             opts,
		iter:           qs.db.NewIterator(nil, opts),
		open:           true,
		qs:             qs,
	}

	ok := it.iter.Seek(it.nextPrefix)
	if !ok {
		it.open = false
		it.iter.Release()
		glog.Error("Opening LevelDB iterator couldn't seek to location ", it.nextPrefix)
	}

	return &it
}

func (it *Iterator) UID() uint64 {
	return it.uid
}

func (it *Iterator) Reset() {
	if !it.open {
		it.iter = it.qs.db.NewIterator(nil, it.ro)
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
	out := NewIterator(it.originalPrefix, it.dir, Token(it.checkID), it.qs)
	out.tags.CopyFrom(it)
	return out
}

func (it *Iterator) Close() error {
	if it.open {
		it.iter.Release()
		it.open = false
	}
	return nil
}

func (it *Iterator) isLiveValue(val []byte) bool {
	var entry IndexEntry
	json.Unmarshal(val, &entry)
	return len(entry.History)%2 != 0
}

func (it *Iterator) Next() bool {
	if it.iter == nil {
		it.result = nil
		return false
	}
	if !it.open {
		it.result = nil
		return false
	}
	if !it.iter.Valid() {
		it.result = nil
		it.Close()
		return false
	}
	if bytes.HasPrefix(it.iter.Key(), it.nextPrefix) {
		if !it.isLiveValue(it.iter.Value()) {
			return it.Next()
		}
		out := make([]byte, len(it.iter.Key()))
		copy(out, it.iter.Key())
		it.result = Token(out)
		ok := it.iter.Next()
		if !ok {
			it.Close()
		}
		return true
	}
	it.Close()
	it.result = nil
	return false
}

func (it *Iterator) Err() error {
	return it.iter.Error()
}

func (it *Iterator) Result() graph.Value {
	return it.result
}

func (it *Iterator) NextPath() bool {
	return false
}

// No subiterators.
func (it *Iterator) SubIterators() []graph.Iterator {
	return nil
}

func PositionOf(prefix []byte, d quad.Direction, qs *QuadStore) int {
	if bytes.Equal(prefix, []byte("sp")) {
		switch d {
		case quad.Subject:
			return 2
		case quad.Predicate:
			return hashSize + 2
		case quad.Object:
			return 2*hashSize + 2
		case quad.Label:
			return 3*hashSize + 2
		}
	}
	if bytes.Equal(prefix, []byte("po")) {
		switch d {
		case quad.Subject:
			return 2*hashSize + 2
		case quad.Predicate:
			return 2
		case quad.Object:
			return hashSize + 2
		case quad.Label:
			return hashSize + 2
		}
	}
	if bytes.Equal(prefix, []byte("os")) {
		switch d {
		case quad.Subject:
			return hashSize + 2
		case quad.Predicate:
			return 2*hashSize + 2
		case quad.Object:
			return 2
		case quad.Label:
			return 3*hashSize + 2
		}
	}
	if bytes.Equal(prefix, []byte("cp")) {
		switch d {
		case quad.Subject:
			return 2*hashSize + 2
		case quad.Predicate:
			return hashSize + 2
		case quad.Object:
			return 3*hashSize + 2
		case quad.Label:
			return 2
		}
	}
	panic("unreachable")
}

func (it *Iterator) Contains(v graph.Value) bool {
	val := v.(Token)
	if val[0] == 'z' {
		return false
	}
	offset := PositionOf(val[0:2], it.dir, it.qs)
	if bytes.HasPrefix(val[offset:], it.checkID[1:]) {
		// You may ask, why don't we check to see if it's a valid (not deleted) quad
		// again?
		//
		// We've already done that -- in order to get the graph.Value token in the
		// first place, we had to have done the check already; it came from a Next().
		//
		// However, if it ever starts coming from somewhere else, it'll be more
		// efficient to change the interface of the graph.Value for LevelDB to a
		// struct with a flag for isValid, to save another random read.
		return true
	}
	return false
}

func (it *Iterator) Size() (int64, bool) {
	return it.qs.SizeOf(Token(it.checkID)), true
}

func (it *Iterator) Describe() graph.Description {
	size, _ := it.Size()
	return graph.Description{
		UID:       it.UID(),
		Name:      it.qs.NameOf(Token(it.checkID)),
		Type:      it.Type(),
		Tags:      it.tags.Tags(),
		Size:      size,
		Direction: it.dir,
	}
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
		ContainsCost: 1,
		NextCost:     2,
		Size:         s,
	}
}

var _ graph.Nexter = &Iterator{}
