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

package kv

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/proto"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/quad/pquads"
	boom "github.com/tylertreat/BoomFilters"
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
)

type FillBucket interface {
	SetFillPercent(v float64)
}

func (qs *QuadStore) createBuckets(upfront bool) error {
	err := Update(qs.db, func(tx BucketTx) error {
		var err error
		for _, index := range buckets {
			_, err = tx.Bucket(index, OpCreate)
			if err != nil {
				return fmt.Errorf("could not create bucket %s: %s", string(index), err)
			}
		}
		b, _ := tx.Bucket(logIndex, OpGet)
		if f, ok := b.(FillBucket); ok {
			f.SetFillPercent(0.9)
		}
		//tx.Bucket(valueIndex).FillPercent = 0.4
		return nil
	})
	if err != nil {
		return err
	}
	if !upfront {
		return nil
	}
	for i := 0; i < 256; i++ {
		err := Update(qs.db, func(tx BucketTx) error {
			var err error
			for j := 0; j < 256; j++ {
				_, err = tx.Bucket(bucketFor(byte(i), byte(j)), OpCreate)
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

func (qs *QuadStore) writeHorizonAndSize(tx BucketTx, horizon, size int64) error {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	if horizon < 0 {
		horizon, size = qs.horizon, qs.size
	}
	b, err := tx.Bucket(metaBucket, OpGet)
	if err != nil {
		return err
	}

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(size))
	err = b.Put([]byte("size"), buf)

	if err != nil {
		clog.Errorf("Couldn't write size!")
		return err
	}

	buf = make([]byte, 8) // bolt needs all slices available on Commit
	binary.LittleEndian.PutUint64(buf, uint64(horizon))
	err = b.Put([]byte("horizon"), buf)

	if err != nil {
		clog.Errorf("Couldn't write horizon!")
		return err
	}
	return err
}

func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta, ignoreOpts graph.IgnoreOpts) error {
	qs.writer.Lock()
	defer qs.writer.Unlock()
	tx, err := qs.db.Tx(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	b, _ := tx.Bucket(logIndex, OpGet)
	if f, ok := b.(FillBucket); ok {
		f.SetFillPercent(0.9)
	}
	qs.mu.RLock()
	size := qs.size
	horizon := qs.horizon
	qs.mu.RUnlock()

nextDelta:
	for _, d := range deltas {
		link := proto.Primitive{}
		mustBeNew := false
		for _, dir := range quad.Directions {
			val := d.Quad.Get(dir)
			if val == nil {
				continue
			}
			v, err := qs.resolveQuadValue(tx, val)
			if err != nil {
				return err
			}
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
				horizon++
				node.ID = uint64(horizon)
				err = qs.index(tx, node, val)
				mustBeNew = true
				if err != nil {
					return err
				}
				v = node.ID
			}
			link.SetDirection(dir, v)
		}
		if d.Action == graph.Delete {
			id, err := qs.hasPrimitive(tx, &link)
			if err != nil {
				return err
			}
			err = qs.markAsDead(tx, id)
			if err != nil {
				return err
			}
			qs.bloomRemove(&link)
			size--
			continue
		}

		// Check if it already exists.
		if !mustBeNew {
			id, err := qs.hasPrimitive(tx, &link)
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
		horizon++
		link.ID = uint64(horizon)
		link.Timestamp = time.Now().UnixNano()
		err = qs.index(tx, &link, nil)
		qs.bloomAdd(&link)
		if err != nil {
			return err
		}
		size++
	}
	err = qs.flushMapBucket(tx)
	if err != nil {
		return err
	}
	err = qs.writeHorizonAndSize(tx, horizon, size)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	qs.mu.Lock()
	qs.size = size
	qs.horizon = horizon
	qs.mu.Unlock()
	return nil
}

func (qs *QuadStore) index(tx BucketTx, p *proto.Primitive, val quad.Value) error {
	if p.IsNode() {
		return qs.indexNode(tx, p, val)
	}
	return qs.indexLink(tx, p)
}

func (qs *QuadStore) indexNode(tx BucketTx, p *proto.Primitive, val quad.Value) error {
	var err error
	if val == nil {
		val, err = pquads.UnmarshalValue(p.Value)
		if err != nil {
			return err
		}
	}
	qs.bufLock.Lock()
	defer qs.bufLock.Unlock()
	quad.HashTo(val, qs.hashBuf)
	bucket, err := tx.Bucket(bucketFor(qs.hashBuf[0], qs.hashBuf[1]), OpUpsert)
	if err != nil {
		return err
	}
	err = bucket.Put(qs.hashBuf, uint64toBytes(p.ID))
	if err != nil {
		return err
	}
	if iri, ok := val.(quad.IRI); ok {
		qs.valueLRU.Put(string(iri), p.ID)
	}
	return qs.addToLog(tx, p)
}

func (qs *QuadStore) indexLink(tx BucketTx, p *proto.Primitive) error {
	var err error
	// Subject
	err = qs.addToMapBucket(tx, "sub", p.Subject, p.ID)
	if err != nil {
		return err
	}
	// Object
	err = qs.addToMapBucket(tx, "obj", p.Object, p.ID)
	if err != nil {
		return err
	}
	err = qs.indexSchema(tx, p)
	if err != nil {
		return err
	}
	return qs.addToLog(tx, p)
}

func (qs *QuadStore) markAsDead(tx BucketTx, id uint64) error {
	p, err := qs.getPrimitiveFromLog(tx, id)
	if err != nil {
		return err
	}
	p.Deleted = true
	//TODO(barakmich): Add tombstone?
	return qs.addToLog(tx, p)
}

func (qs *QuadStore) getBucketIndex(tx BucketTx, bucket []byte, key uint64) ([]uint64, error) {
	b, err := tx.Bucket([]byte(bucket), OpGet)
	if err != nil {
		return nil, err
	}
	kbytes := uint64KeyBytes(key)
	v, err := b.Get(kbytes)
	if err == ErrNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return decodeIndex(v)
}

func decodeIndex(b []byte) ([]uint64, error) {
	r := bytes.NewBuffer(b)
	var err error
	var out []uint64
	for {
		var x uint64
		x, err = binary.ReadUvarint(r)
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

func (qs *QuadStore) hasPrimitive(tx BucketTx, p *proto.Primitive) (uint64, error) {
	if !qs.testBloom(p) {
		return 0, nil
	}
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

func (qs *QuadStore) addToMapBucket(tx BucketTx, bucket string, key, value uint64) error {
	if key == 0 {
		return fmt.Errorf("trying to add to map bucket %s with key 0", bucket)
	}
	if qs.mapBucket == nil {
		qs.mapBucket = make(map[string]map[uint64][]uint64)
	}
	if _, ok := qs.mapBucket[bucket]; !ok {
		qs.mapBucket[bucket] = make(map[uint64][]uint64)
	}

	l := qs.mapBucket[bucket][key]
	qs.mapBucket[bucket][key] = append(l, value)
	return nil
}

func (qs *QuadStore) flushMapBucket(tx BucketTx) error {
	kbytes := make([]byte, 8)
	for bucket, m := range qs.mapBucket {
		var bname []byte
		if bucket == "sub" {
			bname = subjectIndex
		} else if bucket == "obj" {
			bname = objectIndex
		} else {
			return fmt.Errorf("unexpected bucket name: %q", bucket)
		}
		b, err := tx.Bucket(bname, OpGet)
		if err != nil {
			return err
		}
		keys := make(Int64Set, len(m))
		i := 0
		for k := range m {
			keys[i] = k
			i++
		}
		sort.Sort(keys)
		for _, k := range keys {
			l := m[k]
			binary.BigEndian.PutUint64(kbytes, k)
			bytelist, err := b.Get(kbytes)
			if err == ErrNotFound {
				err = nil
			} else if err != nil {
				return err
			}
			buf := appendIndex(bytelist, l)
			err = b.Put(kbytes, buf)
			if err != nil {
				return err
			}
		}
	}
	qs.mapBucket = nil
	return nil
}

func (qs *QuadStore) indexSchema(tx BucketTx, p *proto.Primitive) error {
	return nil
}

func (qs *QuadStore) addToLog(tx BucketTx, p *proto.Primitive) error {
	buf, err := p.Marshal()
	if err != nil {
		return err
	}
	b, err := tx.Bucket(logIndex, OpGet)
	if err != nil {
		return err
	}
	return b.Put(uint64KeyBytes(p.ID), buf)
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

func (qs *QuadStore) resolveQuadValue(tx BucketTx, v quad.Value) (uint64, error) {
	var isIRI bool
	if iri, ok := v.(quad.IRI); ok {
		isIRI = true
		if x, ok := qs.valueLRU.Get(string(iri)); ok {
			return x.(uint64), nil
		}
	}

	qs.bufLock.Lock()
	defer qs.bufLock.Unlock()
	quad.HashTo(v, qs.hashBuf)
	b, err := tx.Bucket(bucketFor(qs.hashBuf[0], qs.hashBuf[1]), OpGet)
	if err == ErrNoBucket {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	val, err := b.Get(qs.hashBuf)
	if err == ErrNotFound {
		return 0, nil
	} else if err != nil {
		return 0, err
	} else if val == nil {
		return 0, nil
	}
	out, _ := binary.Uvarint(val)
	if isIRI {
		qs.valueLRU.Put(string(v.(quad.IRI)), out)
	}
	return out, nil
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

func (qs *QuadStore) getPrimitiveFromLog(tx BucketTx, k uint64) (*proto.Primitive, error) {
	p := &proto.Primitive{}
	b, err := tx.Bucket(logIndex, OpGet)
	if err != nil {
		return nil, err
	}
	v, err := b.Get(uint64KeyBytes(k))
	if err != nil && err != ErrNotFound {
		return nil, err
	} else if v == nil {
		return p, fmt.Errorf("no such log entry")
	}
	err = p.Unmarshal(v)
	return p, err
}

func (qs *QuadStore) initBloomFilter() error {
	qs.exists = boom.NewDeletableBloomFilter(100*1000*1000, 120, 0.05)
	ctx := context.TODO()
	qs.bufLock.Lock()
	defer qs.bufLock.Unlock()
	return View(qs.db, func(tx BucketTx) error {
		p := proto.Primitive{}
		b, err := tx.Bucket(logIndex, OpGet)
		if err != nil {
			return err
		}
		it := b.Scan(nil)
		defer it.Close()
		for it.Next(ctx) {
			v := it.Key()
			p = proto.Primitive{}
			err = p.Unmarshal(v)
			if err != nil {
				return err
			}
			if p.IsNode() {
				continue
			} else if p.Deleted {
				continue
			}
			writePrimToBuf(&p, qs.bloomBuf)
			qs.exists.Add(qs.bloomBuf)
		}
		return it.Err()
	})
}

func (qs *QuadStore) testBloom(p *proto.Primitive) bool {
	qs.bufLock.Lock()
	defer qs.bufLock.Unlock()
	writePrimToBuf(p, qs.bloomBuf)
	return qs.exists.Test(qs.bloomBuf)
}

func (qs *QuadStore) bloomRemove(p *proto.Primitive) {
	qs.bufLock.Lock()
	defer qs.bufLock.Unlock()
	writePrimToBuf(p, qs.bloomBuf)
	qs.exists.TestAndRemove(qs.bloomBuf)
}

func (qs *QuadStore) bloomAdd(p *proto.Primitive) {
	qs.bufLock.Lock()
	defer qs.bufLock.Unlock()
	writePrimToBuf(p, qs.bloomBuf)
	qs.exists.Add(qs.bloomBuf)
}

func writePrimToBuf(p *proto.Primitive, buf []byte) {
	binary.BigEndian.PutUint64(buf[0:8], p.Subject)
	binary.BigEndian.PutUint64(buf[8:16], p.Predicate)
	binary.BigEndian.PutUint64(buf[16:24], p.Object)
}

type Int64Set []uint64

func (a Int64Set) Len() int           { return len(a) }
func (a Int64Set) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Int64Set) Less(i, j int) bool { return a[i] < a[j] }
