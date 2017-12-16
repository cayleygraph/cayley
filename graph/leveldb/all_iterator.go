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

	ldbit "github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
)

type AllIterator struct {
	nodes  bool
	uid    uint64
	tags   graph.Tagger
	prefix []byte
	dir    quad.Direction
	open   bool
	iter   ldbit.Iterator
	qs     *QuadStore
	ro     *opt.ReadOptions
	result graph.Value
}

func NewAllIterator(prefix string, d quad.Direction, qs *QuadStore) *AllIterator {
	opts := &opt.ReadOptions{
		DontFillCache: true,
	}

	it := AllIterator{
		nodes:  prefix == "z",
		uid:    iterator.NextUID(),
		ro:     opts,
		iter:   qs.db.NewIterator(nil, opts),
		prefix: []byte(prefix),
		dir:    d,
		open:   true,
		qs:     qs,
	}

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
		it.iter = it.qs.db.NewIterator(nil, it.ro)
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
	it.tags.TagResult(dst, it.Result())
}

func (it *AllIterator) Clone() graph.Iterator {
	out := NewAllIterator(string(it.prefix), it.dir, it.qs)
	out.tags.CopyFrom(it)
	return out
}

func (it *AllIterator) Next() bool {
	if it.iter == nil {
		it.result = nil
		return false
	} else if !it.open {
		it.result = nil
		return false
	} else if !it.iter.Valid() {
		it.result = nil
		it.Close()
		return false
	}
	for {
		out := clone(it.iter.Key())
		live := it.nodes || isLiveValue(it.iter.Value())
		if !it.iter.Next() {
			it.Close()
			if !live {
				return false
			}
		}
		if !bytes.HasPrefix(out, it.prefix) {
			it.Close()
			return false
		}
		if !live {
			continue
		}
		it.result = Token(out)
		return true
	}
}

func (it *AllIterator) Err() error {
	return it.iter.Error()
}

func (it *AllIterator) Result() graph.Value {
	return it.result
}

func (it *AllIterator) NextPath() bool {
	return false
}

// No subiterators.
func (it *AllIterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *AllIterator) Contains(v graph.Value) bool {
	it.result = v
	return true
}

func (it *AllIterator) Close() error {
	if it.open {
		it.iter.Release()
		it.open = false
	}
	return nil
}

func (it *AllIterator) Size() (int64, bool) {
	size, err := it.qs.SizeOfPrefix(it.prefix)
	if err == nil {
		return size, false
	}
	// INT64_MAX
	return int64(^uint64(0) >> 1), false
}

func (it *AllIterator) String() string {
	return "LeveldbAll"
}

func (it *AllIterator) Type() graph.Type { return graph.All }
func (it *AllIterator) Sorted() bool     { return false }

func (it *AllIterator) Optimize() (graph.Iterator, bool) {
	return it, false
}

func (it *AllIterator) Stats() graph.IteratorStats {
	s, exact := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     2,
		Size:         s,
		ExactSize:    exact,
	}
}

var _ graph.Iterator = &AllIterator{}
