// Copyright 2016 The Cayley Authors. All rights reserved.
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

package bolt2

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/boltdb/bolt"
	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/proto"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/quad/pquads"
)

var (
	metaBucket     = []byte("meta")
	subjectIndex   = []byte{quad.Subject.Prefix()}
	objectIndex    = []byte{quad.Object.Prefix()}
	sameAsIndex    = []byte("sameas")
	sameNodesIndex = []byte("samenodes")
	logIndex       = []byte("log")

	// List of all buckets in the current version of the database.
	buckets = [][]byte{
		metaBucket,
		subjectIndex,
		objectIndex,
		sameAsIndex,
		sameNodesIndex,
		logIndex,
	}
	hashBuf = make([]byte, quad.HashSize)
)

func (qs *QuadStore) createBuckets() error {
	err := qs.db.Update(func(tx *bolt.Tx) error {
		var err error
		for _, index := range buckets {
			_, err = tx.CreateBucket(index)
			if err != nil {
				return fmt.Errorf("could not create bucket %s: %s", string(index), err)
			}
		}
		tx.Bucket(logIndex).FillPercent = 0.8
		//tx.Bucket(valueIndex).FillPercent = 0.4
		return nil
	})
	if err != nil {
		return err
	}
	for i := 0; i < 256; i++ {
		err := qs.db.Update(func(tx *bolt.Tx) error {
			var err error
			for j := 0; j < 256; j++ {
				_, err = tx.CreateBucket(bucketFor(byte(i), byte(j)))
				if err != nil {
					return fmt.Errorf("could not create subbucket %d %d : %s", i, j, err)
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func bucketFor(i, j byte) []byte {
	return []byte{'v', i, j}
}

func (qs *QuadStore) writeHorizonAndSize(tx *bolt.Tx) error {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, qs.size)
	if err != nil {
		clog.Errorf("Couldn't convert size!")
		return err
	}
	b := tx.Bucket(metaBucket)
	werr := b.Put([]byte("size"), buf.Bytes())
	if werr != nil {
		clog.Errorf("Couldn't write size!")
		return werr
	}
	buf.Reset()
	err = binary.Write(buf, binary.LittleEndian, qs.horizon)

	if err != nil {
		clog.Errorf("Couldn't convert horizon!")
	}

	werr = b.Put([]byte("horizon"), buf.Bytes())

	if werr != nil {
		clog.Errorf("Couldn't write horizon!")
		return werr
	}
	return err
}

func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta, ignoreOpts graph.IgnoreOpts) error {
	tx, err := qs.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

nextDelta:
	for _, d := range deltas {
		link := &proto.Primitive{}
		mustBeNew := false
		for _, dir := range quad.Directions {
			val := d.Quad.Get(dir)
			if val == nil {
				continue
			}
			v := qs.resolveQuadValue(tx, val)
			if v == 0 {
				// Not found
				if d.Action == graph.Delete {
					if ignoreOpts.IgnoreMissing {
						continue nextDelta
					}
					return fmt.Errorf("Deleting unknown quad: %s", d.Quad)
				}
				node, err := qs.createNodePrimitive(val)
				if err != nil {
					return err
				}
				qs.horizon++
				node.ID = uint64(qs.horizon)
				err = qs.index(tx, node, val)
				mustBeNew = true
				if err != nil {
					return err
				}
				v = node.ID
			}
			link.SetDirection(dir, v)
		}
		qs.horizon++
		link.ID = uint64(qs.horizon)
		link.Timestamp = time.Now().UnixNano()
		if d.Action == graph.Delete {
			id, err := qs.hasPrimitive(tx, link)
			if err != nil {
				return err
			}
			err = qs.markAsDead(tx, id)
			if err != nil {
				return err
			}
			qs.size--
			continue
		}

		// Check if it already exists.
		if !mustBeNew {
			id, err := qs.hasPrimitive(tx, link)
			if err != nil {
				return err
			}
			if id != 0 {
				if ignoreOpts.IgnoreDup {
					continue
				}
				return fmt.Errorf("adding duplicate link %v", d)
			}
		}
		err = qs.index(tx, link, nil)
		if err != nil {
			return err
		}
		qs.size++
	}
	err = qs.flushMapBucket(tx)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (qs *QuadStore) index(tx *bolt.Tx, p *proto.Primitive, val quad.Value) error {
	if p.IsNode() {
		return qs.indexNode(tx, p, val)
	}
	return qs.indexLink(tx, p)
}

func (qs *QuadStore) indexNode(tx *bolt.Tx, p *proto.Primitive, val quad.Value) error {
	var err error
	if val == nil {
		val, err = pquads.UnmarshalValue(p.Value)
		if err != nil {
			return err
		}
	}
	quad.HashTo(val, hashBuf)
	bucket := tx.Bucket(bucketFor(hashBuf[0], hashBuf[1]))
	err = bucket.Put(hashBuf, uint64KeyBytes(p.ID))
	if err != nil {
		return err
	}
	if iri, ok := val.(quad.IRI); ok {
		qs.valueLRU.Put(string(iri), p.ID)
	}
	return qs.addToLog(tx, p)
}

func (qs *QuadStore) indexLink(tx *bolt.Tx, p *proto.Primitive) error {
	var err error
	// Subject
	err = qs.addToMapBucket(tx, subjectIndex, p.Subject, p.ID)
	if err != nil {
		return err
	}
	// Object
	err = qs.addToMapBucket(tx, objectIndex, p.Object, p.ID)
	if err != nil {
		return err
	}
	err = qs.indexSchema(tx, p)
	if err != nil {
		return err
	}
	return qs.addToLog(tx, p)
}

func (qs *QuadStore) markAsDead(tx *bolt.Tx, id uint64) error {
	p, err := qs.getPrimitiveFromLog(tx, id)
	if err != nil {
		return err
	}
	p.Deleted = true
	//TODO(barakmich): Add tombstone?
	return qs.addToLog(tx, p)
}

func (qs *QuadStore) getBucketIndex(tx *bolt.Tx, bucket []byte, key uint64) ([]uint64, error) {
	b := tx.Bucket([]byte(bucket))
	kbytes := uint64KeyBytes(key)
	v := b.Get(kbytes)
	return decodeIndex(v)
}

func decodeIndex(b []byte) ([]uint64, error) {
	r := bytes.NewBuffer(b)
	var err error
	var out []uint64
	for {
		var x uint64
		x, err := binary.ReadUvarint(r)
		if err != nil {
			break
		}
		out = append(out, x)
	}
	if err != nil && err != io.EOF {
		return nil, err
	}
	return out, nil
}

func appendIndex(bytelist []byte, l []uint64) []byte {
	b := make([]byte, len(bytelist)+(binary.MaxVarintLen64*len(l)))
	copy(b[:len(bytelist)], bytelist)
	off := len(bytelist)
	for _, x := range l {
		n := binary.PutUvarint(b[off:], x)
		off += n
	}
	return b[:off]
}

func (qs *QuadStore) hasPrimitive(tx *bolt.Tx, p *proto.Primitive) (uint64, error) {
	sub, err := qs.getBucketIndex(tx, subjectIndex, p.Subject)
	if err != nil {
		return 0, err
	}
	obj, err := qs.getBucketIndex(tx, objectIndex, p.Object)
	if err != nil {
		return 0, err
	}
	options := intersectSortedUint64(sub, obj)
	for _, x := range options {
		prim, err := qs.getPrimitiveFromLog(tx, x)
		if err != nil {
			return 0, err
		}
		if prim.IsSameLink(p) {
			return prim.ID, nil
		}
	}
	return 0, nil
}

func intersectSortedUint64(a, b []uint64) []uint64 {
	var c []uint64
	boff := 0
outer:
	for _, x := range a {
		for {
			if boff >= len(b) {
				break outer
			}
			if x > b[boff] {
				boff++
				continue
			}
			if x < b[boff] {
				break
			}
			if x == b[boff] {
				c = append(c, x)
				boff++
				break
			}
		}
	}
	return c
}

func (qs *QuadStore) addToMapBucket(tx *bolt.Tx, bucket []byte, key, value uint64) error {
	if key == 0 {
		return fmt.Errorf("trying to add to map bucket %s with key 0", bucket)
	}
	if qs.mapBucket == nil {
		qs.mapBucket = make(map[string]map[uint64][]uint64)
	}
	if _, ok := qs.mapBucket[string(bucket)]; !ok {
		qs.mapBucket[string(bucket)] = make(map[uint64][]uint64)
	}

	l := qs.mapBucket[string(bucket)][key]
	qs.mapBucket[string(bucket)][key] = append(l, value)
	return nil
}

func (qs *QuadStore) flushMapBucket(tx *bolt.Tx) error {
	for bucket, m := range qs.mapBucket {
		b := tx.Bucket([]byte(bucket))
		keys := make(Int64Set, len(m))
		i := 0
		for k := range m {
			keys[i] = k
			i++
		}
		sort.Sort(keys)
		for _, k := range keys {
			l := m[k]
			kbytes := uint64KeyBytes(k)
			bytelist := b.Get(kbytes)
			bytes := appendIndex(bytelist, l)
			err := b.Put(kbytes, bytes)
			if err != nil {
				return err
			}
		}
	}
	qs.mapBucket = nil
	return nil
}

func (qs *QuadStore) indexSchema(tx *bolt.Tx, p *proto.Primitive) error {
	return nil
}

func (qs *QuadStore) addToLog(tx *bolt.Tx, p *proto.Primitive) error {
	b, err := p.Marshal()
	if err != nil {
		return err
	}
	return tx.Bucket(logIndex).Put(uint64KeyBytes(p.ID), b)
}

func (qs *QuadStore) createNodePrimitive(v quad.Value) (*proto.Primitive, error) {
	p := &proto.Primitive{}
	b, err := pquads.MarshalValue(v)
	if err != nil {
		return p, err
	}
	p.Value = b
	p.Timestamp = time.Now().UnixNano()
	return p, nil
}

func (qs *QuadStore) resolveQuadValue(tx *bolt.Tx, v quad.Value) uint64 {
	var isIRI bool
	if iri, ok := v.(quad.IRI); ok {
		isIRI = true
		if x, ok := qs.valueLRU.Get(string(iri)); ok {
			return x.(uint64)
		}
	}

	quad.HashTo(v, hashBuf)
	buck := tx.Bucket(bucketFor(hashBuf[0], hashBuf[1]))
	if buck == nil {
		return 0
	}
	val := buck.Get(hashBuf)
	if val == nil {
		return 0
	}
	out := binary.BigEndian.Uint64(val)
	if isIRI {
		qs.valueLRU.Put(string(v.(quad.IRI)), out)
	}
	return out
}

func uint64toBytes(x uint64) []byte {
	b := make([]byte, binary.MaxVarintLen64)
	return uint64toBytesAt(x, b)
}

func uint64toBytesAt(x uint64, bytes []byte) []byte {
	n := binary.PutUvarint(bytes, x)
	return bytes[:n]
}

func uint64KeyBytes(x uint64) []byte {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, x)
	return k
}

func (qs *QuadStore) getPrimitiveFromLog(tx *bolt.Tx, k uint64) (*proto.Primitive, error) {
	p := &proto.Primitive{}
	b := tx.Bucket(logIndex).Get(uint64KeyBytes(k))
	if b == nil {
		return p, fmt.Errorf("no such log entry")
	}
	err := p.Unmarshal(b)
	return p, err
}

type Int64Set []uint64

func (a Int64Set) Len() int           { return len(a) }
func (a Int64Set) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Int64Set) Less(i, j int) bool { return a[i] < a[j] }
