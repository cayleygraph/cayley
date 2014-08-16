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
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"

	"github.com/barakmich/glog"
	"github.com/boltdb/bolt"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

func init() {
	graph.RegisterTripleStore("bolt", true, newQuadStore, createNewBolt)
}

type Token struct {
	bucket []byte
	key    []byte
}

func (t *Token) Key() interface{} {
	return fmt.Sprint(t.bucket, t.key)
}

type QuadStore struct {
	db         *bolt.DB
	path       string
	open       bool
	size       int64
	horizon    int64
	makeHasher func() hash.Hash
	hasherSize int
}

func createNewBolt(path string, _ graph.Options) error {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		glog.Errorf("Error: couldn't create Bolt database: %v", err)
		return err
	}
	defer db.Close()
	qs := &QuadStore{}
	qs.db = db
	err = qs.createBuckets()
	if err != nil {
		return err
	}
	qs.Close()
	return nil
}

func newQuadStore(path string, options graph.Options) (graph.TripleStore, error) {
	var qs QuadStore
	var err error
	qs.hasherSize = sha1.Size
	qs.makeHasher = sha1.New
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		glog.Errorln("Error, couldn't open! ", err)
		return nil, err
	}
	qs.db = db
	err = qs.getMetadata()
	if err != nil {
		return nil, err
	}
	return &qs, nil
}

func (qs *QuadStore) createBuckets() error {
	return qs.db.Update(func(tx *bolt.Tx) error {
		var err error
		for _, index := range [][4]quad.Direction{spo, osp, pos, cps} {
			_, err = tx.CreateBucket(bucketFor(index))
			if err != nil {
				return fmt.Errorf("Couldn't create bucket: %s", err)
			}
		}
		_, err = tx.CreateBucket(logBucket)
		if err != nil {
			return fmt.Errorf("Couldn't create bucket: %s", err)
		}
		_, err = tx.CreateBucket(nodeBucket)
		if err != nil {
			return fmt.Errorf("Couldn't create bucket: %s", err)
		}
		_, err = tx.CreateBucket(metaBucket)
		if err != nil {
			return fmt.Errorf("Couldn't create bucket: %s", err)
		}
		return nil
	})
}

func (qs *QuadStore) Size() int64 {
	return qs.size
}

func (qs *QuadStore) Horizon() int64 {
	return qs.horizon
}

func (qs *QuadStore) createDeltaKeyFor(id int64) []byte {
	return []byte(fmt.Sprintf("%018x", id))
}

func bucketFor(d [4]quad.Direction) []byte {
	return []byte{d[0].Prefix(), d[1].Prefix(), d[2].Prefix(), d[3].Prefix()}
}

func (qs *QuadStore) createKeyFor(d [4]quad.Direction, triple quad.Quad) []byte {
	hasher := qs.makeHasher()
	key := make([]byte, 0, (qs.hasherSize * 4))
	key = append(key, qs.convertStringToByteHash(triple.Get(d[0]), hasher)...)
	key = append(key, qs.convertStringToByteHash(triple.Get(d[1]), hasher)...)
	key = append(key, qs.convertStringToByteHash(triple.Get(d[2]), hasher)...)
	key = append(key, qs.convertStringToByteHash(triple.Get(d[3]), hasher)...)
	return key
}

func (qs *QuadStore) createValueKeyFor(s string) []byte {
	hasher := qs.makeHasher()
	key := make([]byte, 0, qs.hasherSize)
	key = append(key, qs.convertStringToByteHash(s, hasher)...)
	return key
}

type IndexEntry struct {
	History []int64
}

// Short hand for direction permutations.
var (
	spo       = [4]quad.Direction{quad.Subject, quad.Predicate, quad.Object, quad.Label}
	osp       = [4]quad.Direction{quad.Object, quad.Subject, quad.Predicate, quad.Label}
	pos       = [4]quad.Direction{quad.Predicate, quad.Object, quad.Subject, quad.Label}
	cps       = [4]quad.Direction{quad.Label, quad.Predicate, quad.Subject, quad.Object}
	spoBucket = bucketFor(spo)
	ospBucket = bucketFor(osp)
	posBucket = bucketFor(pos)
	cpsBucket = bucketFor(cps)
)

var logBucket = []byte("log")
var nodeBucket = []byte("node")
var metaBucket = []byte("meta")

func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta) error {
	var old_size = qs.size
	var old_horizon = qs.horizon
	var size_change int64
	err := qs.db.Update(func(tx *bolt.Tx) error {
		var b *bolt.Bucket
		resizeMap := make(map[string]int64)
		size_change = int64(0)
		for _, d := range deltas {
			bytes, err := json.Marshal(d)
			if err != nil {
				return err
			}
			b = tx.Bucket(logBucket)
			err = b.Put(qs.createDeltaKeyFor(d.ID), bytes)
			if err != nil {
				return err
			}
		}
		for _, d := range deltas {
			err := qs.buildQuadWrite(tx, d.Quad, d.ID, d.Action == graph.Add)
			if err != nil {
				return err
			}
			delta := int64(1)
			if d.Action == graph.Delete {
				delta = int64(-1)
			}
			resizeMap[d.Quad.Subject] += delta
			resizeMap[d.Quad.Predicate] += delta
			resizeMap[d.Quad.Object] += delta
			if d.Quad.Label != "" {
				resizeMap[d.Quad.Label] += delta
			}
			size_change += delta
			qs.horizon = d.ID
		}
		for k, v := range resizeMap {
			if v != 0 {
				err := qs.UpdateValueKeyBy(k, v, tx)
				if err != nil {
					return err
				}
			}
		}
		qs.size += size_change
		return qs.WriteHorizonAndSize(tx)
	})

	if err != nil {
		glog.Error("Couldn't write to DB for Delta set. Error: ", err)
		qs.horizon = old_horizon
		qs.size = old_size
		return err
	}
	return nil
}

func (qs *QuadStore) buildQuadWrite(tx *bolt.Tx, q quad.Quad, id int64, isAdd bool) error {
	var entry IndexEntry
	b := tx.Bucket(spoBucket)
	data := b.Get(qs.createKeyFor(spo, q))
	if data != nil {
		// We got something.
		err := json.Unmarshal(data, &entry)
		if err != nil {
			return err
		}
	}

	if isAdd && len(entry.History)%2 == 1 {
		glog.Error("Adding a valid triple ", entry)
		return graph.ErrQuadExists
	}
	if !isAdd && len(entry.History)%2 == 0 {
		glog.Error("Deleting an invalid triple ", entry)
		return graph.ErrQuadNotExist
	}

	entry.History = append(entry.History, id)

	jsonbytes, err := json.Marshal(entry)
	if err != nil {
		glog.Errorf("Couldn't write to buffer for entry %#v: %s", entry, err)
		return err
	}
	for _, index := range [][4]quad.Direction{spo, osp, pos, cps} {
		if index == cps && q.Get(quad.Label) == "" {
			continue
		}
		b := tx.Bucket(bucketFor(index))
		err = b.Put(qs.createKeyFor(index, q), jsonbytes)
		if err != nil {
			return err
		}
	}
	return nil
}

type ValueData struct {
	Name string
	Size int64
}

func (qs *QuadStore) UpdateValueKeyBy(name string, amount int64, tx *bolt.Tx) error {
	value := ValueData{name, amount}
	b := tx.Bucket(nodeBucket)
	key := qs.createValueKeyFor(name)
	data := b.Get(key)

	if data != nil {
		// Node exists in the database -- unmarshal and update.
		err := json.Unmarshal(data, &value)
		if err != nil {
			glog.Errorf("Error: couldn't reconstruct value: %v", err)
			return err
		}
		value.Size += amount
	}

	// Are we deleting something?
	if value.Size <= 0 {
		value.Size = 0
	}

	// Repackage and rewrite.
	bytes, err := json.Marshal(&value)
	if err != nil {
		glog.Errorf("Couldn't write to buffer for value %s: %s", name, err)
		return err
	}
	err = b.Put(key, bytes)
	return err
}

func (qs *QuadStore) WriteHorizonAndSize(tx *bolt.Tx) error {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, qs.size)
	if err == nil {
		b := tx.Bucket(metaBucket)
		werr := b.Put([]byte("size"), buf.Bytes())
		if werr != nil {
			glog.Error("Couldn't write size!")
			return werr
		}
	} else {
		glog.Errorf("Couldn't convert size!")
		return err
	}
	buf.Reset()
	err = binary.Write(buf, binary.LittleEndian, qs.horizon)
	if err == nil {
		b := tx.Bucket(metaBucket)
		werr := b.Put([]byte("horizon"), buf.Bytes())
		if werr != nil {
			glog.Error("Couldn't write horizon!")
			return werr
		}
	} else {
		glog.Errorf("Couldn't convert horizon!")
	}
	return err
}

func (qs *QuadStore) Close() {
	qs.db.Update(func(tx *bolt.Tx) error {
		return qs.WriteHorizonAndSize(tx)
	})
	qs.db.Close()
	qs.open = false
}

func (qs *QuadStore) Quad(k graph.Value) quad.Quad {
	var in IndexEntry
	var q quad.Quad
	tok := k.(*Token)
	err := qs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(tok.bucket)
		data := b.Get(tok.key)
		if data == nil {
			return nil
		}
		err := json.Unmarshal(data, &in)
		if err != nil {
			return err
		}
		if len(in.History) == 0 {
			return nil
		}
		b = tx.Bucket(logBucket)
		data = b.Get(qs.createDeltaKeyFor(in.History[len(in.History)-1]))
		if data == nil {
			// No harm, no foul.
			return nil
		}
		return json.Unmarshal(data, &q)
	})
	if err != nil {
		glog.Error("Error getting triple: ", err)
		return quad.Quad{}
	}
	return q
}

func (qs *QuadStore) convertStringToByteHash(s string, hasher hash.Hash) []byte {
	hasher.Reset()
	key := make([]byte, 0, qs.hasherSize)
	hasher.Write([]byte(s))
	key = hasher.Sum(key)
	return key
}

func (qs *QuadStore) ValueOf(s string) graph.Value {
	return &Token{
		bucket: nodeBucket,
		key:    qs.createValueKeyFor(s),
	}
}

func (qs *QuadStore) valueData(t *Token) ValueData {
	var out ValueData
	if glog.V(3) {
		glog.V(3).Infof("%s %v", string(t.bucket), t.key)
	}
	err := qs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(t.bucket)
		data := b.Get(t.key)
		if data != nil {
			return json.Unmarshal(data, &out)
		}
		return nil
	})
	if err != nil {
		glog.Errorln("Error: couldn't get value")
		return ValueData{}
	}
	return out
}

func (qs *QuadStore) NameOf(k graph.Value) string {
	if k == nil {
		glog.V(2).Info("k was nil")
		return ""
	}
	return qs.valueData(k.(*Token)).Name
}

func (qs *QuadStore) SizeOf(k graph.Value) int64 {
	if k == nil {
		return 0
	}
	return int64(qs.valueData(k.(*Token)).Size)
}

func (qs *QuadStore) getInt64ForKey(tx *bolt.Tx, key string, empty int64) (int64, error) {
	var out int64
	b := tx.Bucket(metaBucket)
	data := b.Get([]byte(key))
	if data == nil {
		return empty, nil
	}
	buf := bytes.NewBuffer(data)
	err := binary.Read(buf, binary.LittleEndian, &out)
	if err != nil {
		return 0, err
	}
	return out, nil
}

func (qs *QuadStore) getMetadata() error {
	err := qs.db.View(func(tx *bolt.Tx) error {
		var err error
		qs.size, err = qs.getInt64ForKey(tx, "size", 0)
		if err != nil {
			return err
		}
		qs.horizon, err = qs.getInt64ForKey(tx, "horizon", 0)
		return err
	})
	return err
}

func (qs *QuadStore) TripleIterator(d quad.Direction, val graph.Value) graph.Iterator {
	var bucket []byte
	switch d {
	case quad.Subject:
		bucket = spoBucket
	case quad.Predicate:
		bucket = posBucket
	case quad.Object:
		bucket = ospBucket
	case quad.Label:
		bucket = cpsBucket
	default:
		panic("unreachable " + d.String())
	}
	return NewIterator(bucket, d, val, qs)
}

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator(nodeBucket, quad.Any, qs)
}

func (qs *QuadStore) TriplesAllIterator() graph.Iterator {
	return NewAllIterator(posBucket, quad.Predicate, qs)
}

func (qs *QuadStore) TripleDirection(val graph.Value, d quad.Direction) graph.Value {
	v := val.(*Token)
	offset := PositionOf(v, d, qs)
	if offset != -1 {
		return &Token{
			bucket: nodeBucket,
			key:    v.key[offset : offset+qs.hasherSize],
		}
	} else {
		return qs.ValueOf(qs.Quad(v).Get(d))
	}
}

func compareTokens(a, b graph.Value) bool {
	atok := a.(*Token)
	btok := b.(*Token)
	return bytes.Equal(atok.key, btok.key) && bytes.Equal(atok.bucket, btok.bucket)
}

func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixedIteratorWithCompare(compareTokens)
}
