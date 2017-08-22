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
	metaBucket = []byte("meta")
	logIndex   = []byte("log")

	// List of all buckets in the current version of the database.
	buckets = [][]byte{
		metaBucket,
		logIndex,
	}

	DefaultQuadIndexes = []QuadIndex{
		{Dirs: []quad.Direction{quad.Subject}},
		{Dirs: []quad.Direction{quad.Object}},
	}
)

var quadKeyEnc = binary.BigEndian

type QuadIndex struct {
	Dirs   []quad.Direction
	Unique bool
}

func (ind QuadIndex) Key(vals []uint64) []byte {
	key := make([]byte, 8*len(vals))
	n := 0
	for i := range vals {
		quadKeyEnc.PutUint64(key[n:], vals[i])
		n += 8
	}
	return key
}
func (ind QuadIndex) KeyFor(p *proto.Primitive) []byte {
	key := make([]byte, 8*len(ind.Dirs))
	n := 0
	for _, d := range ind.Dirs {
		quadKeyEnc.PutUint64(key[n:], p.GetDirection(d))
		n += 8
	}
	return key
}
func (ind QuadIndex) Bucket() []byte {
	b := make([]byte, len(ind.Dirs))
	for i, d := range ind.Dirs {
		b[i] = d.Prefix()
	}
	return b
}

type FillBucket interface {
	SetFillPercent(v float64)
}

func bucketForVal(i, j byte) []byte {
	return []byte{'v', i, j}
}

func (qs *QuadStore) createBuckets(upfront bool) error {
	err := Update(qs.db, func(tx BucketTx) error {
		for _, index := range buckets {
			_ = tx.Bucket(index)
		}
		b := tx.Bucket(logIndex)
		if f, ok := b.(FillBucket); ok {
			f.SetFillPercent(0.9)
		}
		for _, ind := range qs.indexes.all {
			_ = tx.Bucket(ind.Bucket())
		}
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
			for j := 0; j < 256; j++ {
				_ = tx.Bucket(bucketForVal(byte(i), byte(j)))
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (qs *QuadStore) writeHorizonAndSize(tx BucketTx, horizon, size int64) error {
	qs.meta.Lock()
	defer qs.meta.Unlock()
	if horizon < 0 {
		horizon, size = qs.meta.horizon, qs.meta.size
	}
	b := tx.Bucket(metaBucket)

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(size))
	err := b.Put([]byte("size"), buf)

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
	b := tx.Bucket(logIndex)
	if f, ok := b.(FillBucket); ok {
		f.SetFillPercent(0.9)
	}
	qs.meta.RLock()
	size := qs.meta.size
	horizon := qs.meta.horizon
	qs.meta.RUnlock()

	qvals := make([]quad.Value, 4)
nextDelta:
	for _, d := range deltas {
		link := proto.Primitive{}
		mustBeNew := false
		qvals = qvals[:0]
		for _, dir := range quad.Directions {
			if val := d.Quad.Get(dir); val != nil {
				qvals = append(qvals, val)
			}
		}
		ids, err := qs.resolveQuadValues(tx, qvals)
		if err != nil {
			return err
		}
		for _, dir := range quad.Directions {
			if val := d.Quad.Get(dir); val != nil {
				v := ids[0]
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
				ids = ids[1:]
			}
		}
		if d.Action == graph.Delete {
			p, err := qs.hasPrimitive(tx, &link, true)
			if err != nil {
				return err
			} else if p == nil {
				continue
			}
			err = qs.markAsDead(tx, p)
			if err != nil {
				return err
			}
			qs.bloomRemove(&link)
			size--
			continue
		}

		// Check if it already exists.
		if !mustBeNew {
			p, err := qs.hasPrimitive(tx, &link, false)
			if err != nil {
				return err
			}
			if p != nil {
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
	qs.meta.Lock()
	qs.meta.size = size
	qs.meta.horizon = horizon
	qs.meta.Unlock()
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
	hash := quad.HashOf(val)
	bucket := tx.Bucket(bucketForVal(hash[0], hash[1]))
	err = bucket.Put(hash, uint64toBytes(p.ID))
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
	qs.indexes.RLock()
	all := qs.indexes.all
	qs.indexes.RUnlock()
	for _, ind := range all {
		err = qs.addToMapBucket(tx, ind.Bucket(), ind.KeyFor(p), p.ID)
		if err != nil {
			return err
		}
	}
	err = qs.indexSchema(tx, p)
	if err != nil {
		return err
	}
	return qs.addToLog(tx, p)
}

func (qs *QuadStore) markAsDead(tx BucketTx, p *proto.Primitive) error {
	p.Deleted = true
	//TODO(barakmich): Add tombstone?
	return qs.addToLog(tx, p)
}

func (qs *QuadStore) getBucketIndexes(tx BucketTx, keys []BucketKey) ([][]uint64, error) {
	vals, err := tx.Get(keys)
	if err != nil {
		return nil, err
	}
	out := make([][]uint64, len(keys))
	for i, v := range vals {
		if len(v) == 0 {
			continue
		}
		ind, err := decodeIndex(v)
		if err != nil {
			return out, err
		}
		out[i] = ind
	}
	return out, nil
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

func (qs *QuadStore) bestUnique() ([]QuadIndex, error) {
	qs.indexes.RLock()
	ind := qs.indexes.exists
	qs.indexes.RUnlock()
	if len(ind) != 0 {
		return ind, nil
	}
	qs.indexes.Lock()
	defer qs.indexes.Unlock()
	if len(qs.indexes.exists) != 0 {
		return qs.indexes.exists, nil
	}
	for _, in := range qs.indexes.all {
		if in.Unique {
			if clog.V(2) {
				clog.Infof("using unique index: %v", in.Dirs)
			}
			qs.indexes.exists = []QuadIndex{in}
			return qs.indexes.exists, nil
		}
	}
	// TODO: find best combination of indexes
	inds := qs.indexes.all
	if len(inds) == 0 {
		return nil, fmt.Errorf("no indexes defined")
	}
	if clog.V(2) {
		clog.Infof("using index intersection: %v", inds)
	}
	qs.indexes.exists = inds
	return qs.indexes.exists, nil
}

func (qs *QuadStore) hasPrimitive(tx BucketTx, p *proto.Primitive, get bool) (*proto.Primitive, error) {
	if !qs.testBloom(p) {
		return nil, nil
	}
	inds, err := qs.bestUnique()
	if err != nil {
		return nil, err
	}
	unique := len(inds) != 0 && inds[0].Unique
	keys := make([]BucketKey, len(inds))
	for i, in := range inds {
		keys[i] = BucketKey{
			Bucket: in.Bucket(),
			Key:    in.KeyFor(p),
		}
	}
	lists, err := qs.getBucketIndexes(tx, keys)
	if err != nil {
		return nil, err
	}
	var options []uint64
	for len(lists) > 0 {
		if len(lists) == 1 {
			options = lists[0]
			break
		}
		a, b := lists[0], lists[1]
		lists = lists[1:]
		a = intersectSortedUint64(a, b)
		lists[0] = a
	}
	if !get && unique {
		return p, nil
	}
	for _, x := range options {
		// TODO: batch
		prim, err := qs.getPrimitiveFromLog(tx, x)
		if err != nil {
			return nil, err
		}
		if prim.IsSameLink(p) {
			return prim, nil
		}
	}
	return nil, nil
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

func (qs *QuadStore) addToMapBucket(tx BucketTx, bucket []byte, key []byte, value uint64) error {
	if len(key) == 0 {
		return fmt.Errorf("trying to add to map bucket %s with key 0", bucket)
	}
	if qs.mapBucket == nil {
		qs.mapBucket = make(map[string]map[string][]uint64)
	}
	m, ok := qs.mapBucket[string(bucket)]
	if !ok {
		m = make(map[string][]uint64)
		qs.mapBucket[string(bucket)] = m
	}
	m[string(key)] = append(m[string(key)], value)
	return nil
}

func (qs *QuadStore) flushMapBucket(tx BucketTx) error {
	for bucket, m := range qs.mapBucket {
		b := tx.Bucket([]byte(bucket))
		keys := make([][]byte, 0, len(m))
		for k := range m {
			keys = append(keys, []byte(k))
		}
		sort.Slice(keys, func(i, j int) bool {
			return bytes.Compare(keys[i], keys[j]) < 0
		})
		vals, err := b.Get(keys)
		if err != nil {
			return err
		}
		for i, k := range keys {
			l := m[string(k)]
			list := vals[i]
			buf := appendIndex(list, l)
			err = b.Put(keys[i], buf)
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
	b := tx.Bucket(logIndex)
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
	out, err := qs.resolveQuadValues(tx, []quad.Value{v})
	if err != nil {
		return 0, err
	}
	return out[0], nil
}

func bucketKeyForVal(v quad.Value) BucketKey {
	hash := quad.HashOf(v)
	return BucketKey{
		Bucket: bucketForVal(hash[0], hash[1]),
		Key:    hash,
	}
}

func (qs *QuadStore) resolveQuadValues(tx BucketTx, vals []quad.Value) ([]uint64, error) {
	out := make([]uint64, len(vals))
	inds := make([]int, 0, len(vals))
	keys := make([]BucketKey, 0, len(vals))
	for i, v := range vals {
		if iri, ok := v.(quad.IRI); ok {
			if x, ok := qs.valueLRU.Get(string(iri)); ok {
				out[i] = x.(uint64)
				continue
			}
		} else if v == nil {
			continue
		}
		inds = append(inds, i)
		keys = append(keys, bucketKeyForVal(v))
	}
	if len(keys) == 0 {
		return out, nil
	}
	resp, err := tx.Get(keys)
	if err != nil {
		return out, err
	}
	for i, b := range resp {
		if len(b) == 0 {
			continue
		}
		ind := inds[i]
		out[ind], _ = binary.Uvarint(b)
		if iri, ok := vals[ind].(quad.IRI); ok && out[ind] != 0 {
			qs.valueLRU.Put(string(iri), uint64(out[ind]))
		}
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
	quadKeyEnc.PutUint64(k, x)
	return k
}

func (qs *QuadStore) getPrimitivesFromLog(tx BucketTx, keys []uint64) ([]*proto.Primitive, error) {
	b := tx.Bucket(logIndex)
	bkeys := make([][]byte, len(keys))
	for i, k := range keys {
		bkeys[i] = uint64KeyBytes(k)
	}
	vals, err := b.Get(bkeys)
	if err != nil {
		return nil, err
	}
	out := make([]*proto.Primitive, len(keys))
	var last error
	for i, v := range vals {
		if v == nil {
			continue
		}
		var p proto.Primitive
		if err = p.Unmarshal(v); err != nil {
			last = err
		} else {
			out[i] = &p
		}
	}
	return out, last
}

func (qs *QuadStore) getPrimitiveFromLog(tx BucketTx, k uint64) (*proto.Primitive, error) {
	out, err := qs.getPrimitivesFromLog(tx, []uint64{k})
	if err != nil {
		return nil, err
	} else if out[0] == nil {
		return nil, ErrNotFound
	}
	return out[0], nil
}

func (qs *QuadStore) initBloomFilter() error {
	qs.exists.buf = make([]byte, 3*8)
	qs.exists.DeletableBloomFilter = boom.NewDeletableBloomFilter(100*1000*1000, 120, 0.05)
	ctx := context.TODO()
	return View(qs.db, func(tx BucketTx) error {
		p := proto.Primitive{}
		b := tx.Bucket(logIndex)
		it := b.Scan(nil)
		defer it.Close()
		for it.Next(ctx) {
			v := it.Key()
			p = proto.Primitive{}
			err := p.Unmarshal(v)
			if err != nil {
				return err
			}
			if p.IsNode() {
				continue
			} else if p.Deleted {
				continue
			}
			writePrimToBuf(&p, qs.exists.buf)
			qs.exists.Add(qs.exists.buf)
		}
		return it.Err()
	})
}

func (qs *QuadStore) testBloom(p *proto.Primitive) bool {
	qs.exists.Lock()
	defer qs.exists.Unlock()
	writePrimToBuf(p, qs.exists.buf)
	return qs.exists.Test(qs.exists.buf)
}

func (qs *QuadStore) bloomRemove(p *proto.Primitive) {
	qs.exists.Lock()
	defer qs.exists.Unlock()
	writePrimToBuf(p, qs.exists.buf)
	qs.exists.TestAndRemove(qs.exists.buf)
}

func (qs *QuadStore) bloomAdd(p *proto.Primitive) {
	qs.exists.Lock()
	defer qs.exists.Unlock()
	writePrimToBuf(p, qs.exists.buf)
	qs.exists.Add(qs.exists.buf)
}

func writePrimToBuf(p *proto.Primitive, buf []byte) {
	quadKeyEnc.PutUint64(buf[0:8], p.Subject)
	quadKeyEnc.PutUint64(buf[8:16], p.Predicate)
	quadKeyEnc.PutUint64(buf[16:24], p.Object)
}

type Int64Set []uint64

func (a Int64Set) Len() int           { return len(a) }
func (a Int64Set) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Int64Set) Less(i, j int) bool { return a[i] < a[j] }
