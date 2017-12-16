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

package bolt

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/cayleygraph/cayley/clog"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
)

var _ graph.Iterator = &Iterator{}

var (
	bufferSize  = 50
	errNotExist = errors.New("quad does not exist")
)

type Iterator struct {
	uid     uint64
	tags    graph.Tagger
	bucket  []byte
	checkID []byte
	dir     quad.Direction
	qs      *QuadStore
	result  *Token
	buffer  [][]byte
	offset  int
	done    bool
	size    int64
	err     error
}

func NewIterator(bucket []byte, d quad.Direction, value graph.Value, qs *QuadStore) *Iterator {
	tok := value.(*Token)
	if !bytes.Equal(tok.bucket, nodeBucket) {
		clog.Errorf("creating an iterator from a non-node value")
		return &Iterator{done: true}
	}

	it := Iterator{
		uid:    iterator.NextUID(),
		bucket: bucket,
		dir:    d,
		qs:     qs,
		size:   qs.SizeOf(value),
	}

	it.checkID = make([]byte, len(tok.key))
	copy(it.checkID, tok.key)

	return &it
}

func (it *Iterator) UID() uint64 {
	return it.uid
}

func (it *Iterator) Reset() {
	it.buffer = nil
	it.offset = 0
	it.done = false
}

func (it *Iterator) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *Iterator) TagResults(dst map[string]graph.Value) {
	it.tags.TagResult(dst, it.Result())
}

func (it *Iterator) Clone() graph.Iterator {
	out := NewIterator(it.bucket, it.dir, &Token{true, nodeBucket, it.checkID}, it.qs)
	out.Tagger().CopyFrom(it)
	return out
}

func (it *Iterator) Close() error {
	it.result = nil
	it.buffer = nil
	it.done = true
	return nil
}

func (it *Iterator) Next() bool {
	if it.done {
		return false
	}
	if len(it.buffer) <= it.offset+1 {
		it.offset = 0
		var last []byte
		if it.buffer != nil {
			last = it.buffer[len(it.buffer)-1]
		}
		it.buffer = make([][]byte, 0, bufferSize)
		err := it.qs.db.View(func(tx *bolt.Tx) error {
			i := 0
			b := tx.Bucket(it.bucket)
			cur := b.Cursor()
			if last == nil {
				k, v := cur.Seek(it.checkID)
				if bytes.HasPrefix(k, it.checkID) {
					if isLiveValue(v) {
						it.buffer = append(it.buffer, clone(k))
						i++
					}
				} else {
					it.buffer = append(it.buffer, nil)
					return errNotExist
				}
			} else {
				k, _ := cur.Seek(last)
				if !bytes.Equal(k, last) {
					return fmt.Errorf("could not pick up after %v", k)
				}
			}
			for i < bufferSize {
				k, v := cur.Next()
				if k == nil || !bytes.HasPrefix(k, it.checkID) {
					it.buffer = append(it.buffer, nil)
					break
				}
				if !isLiveValue(v) {
					continue
				}
				it.buffer = append(it.buffer, clone(k))
				i++
			}
			return nil
		})
		if err != nil {
			if err != errNotExist {
				clog.Errorf("Error nexting in database: %v", err)
				it.err = err
			}
			it.done = true
			return false
		}
	} else {
		it.offset++
	}
	if it.Result() == nil {
		it.done = true
		return false
	}
	return true
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Result() graph.Value {
	if it.done {
		return nil
	}
	if it.result != nil {
		return it.result
	}
	if it.offset >= len(it.buffer) {
		return nil
	}
	if it.buffer[it.offset] == nil {
		return nil
	}
	return &Token{bucket: it.bucket, key: it.buffer[it.offset]}
}

func (it *Iterator) NextPath() bool {
	return false
}

// No subiterators.
func (it *Iterator) SubIterators() []graph.Iterator {
	return nil
}

func PositionOf(tok *Token, d quad.Direction, qs *QuadStore) int {
	if bytes.Equal(tok.bucket, spoBucket) {
		switch d {
		case quad.Subject:
			return 0
		case quad.Predicate:
			return quad.HashSize
		case quad.Object:
			return 2 * quad.HashSize
		case quad.Label:
			return 3 * quad.HashSize
		}
	}
	if bytes.Equal(tok.bucket, posBucket) {
		switch d {
		case quad.Subject:
			return 2 * quad.HashSize
		case quad.Predicate:
			return 0
		case quad.Object:
			return quad.HashSize
		case quad.Label:
			return 3 * quad.HashSize
		}
	}
	if bytes.Equal(tok.bucket, ospBucket) {
		switch d {
		case quad.Subject:
			return quad.HashSize
		case quad.Predicate:
			return 2 * quad.HashSize
		case quad.Object:
			return 0
		case quad.Label:
			return 3 * quad.HashSize
		}
	}
	if bytes.Equal(tok.bucket, cpsBucket) {
		switch d {
		case quad.Subject:
			return 2 * quad.HashSize
		case quad.Predicate:
			return quad.HashSize
		case quad.Object:
			return 3 * quad.HashSize
		case quad.Label:
			return 0
		}
	}
	panic("unreachable")
}

func (it *Iterator) Contains(v graph.Value) bool {
	val := v.(*Token)
	if bytes.Equal(val.bucket, nodeBucket) {
		return false
	}
	offset := PositionOf(val, it.dir, it.qs)
	if len(val.key) != 0 && bytes.HasPrefix(val.key[offset:], it.checkID) {
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
	return it.size, true
}

func (it *Iterator) String() string {
	return fmt.Sprintf("BoltIter(%q)", it.bucket)
}

func (it *Iterator) Type() graph.Type { return "bolt" }
func (it *Iterator) Sorted() bool     { return false }

func (it *Iterator) Optimize() (graph.Iterator, bool) {
	return it, false
}

func (it *Iterator) Stats() graph.IteratorStats {
	s, exact := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     4,
		Size:         s,
		ExactSize:    exact,
	}
}
