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
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/cayleygraph/cayley/clog"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/proto"
	"github.com/cayleygraph/cayley/quad"
)

func init() {
	graph.RegisterQuadStore(QuadStoreType, graph.QuadStoreRegistration{
		NewFunc:           newQuadStore,
		NewForRequestFunc: nil,
		UpgradeFunc:       upgradeBolt,
		InitFunc:          createNewBolt,
		IsPersistent:      true,
	})
}

var (
	errNoBucket = errors.New("bolt: bucket is missing")
)

const localFillPercent = 0.7

const (
	QuadStoreType = "bolt"
)

var _ graph.Keyer = (*Token)(nil)

type Token struct {
	nodes  bool
	bucket []byte
	key    []byte
}

func (t *Token) IsNode() bool { return t.nodes }

func (t *Token) Key() interface{} {
	return fmt.Sprint(t.bucket, t.key)
}

func clone(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func isLiveValue(val []byte) bool {
	var entry proto.HistoryEntry
	entry.Unmarshal(val)
	return len(entry.History)%2 != 0
}

type QuadStore struct {
	db      *bolt.DB
	path    string
	open    bool
	size    int64
	horizon int64
	version int64
}

func createNewBolt(path string, _ graph.Options) error {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		clog.Errorf("Error: couldn't create Bolt database: %v", err)
		return err
	}
	defer db.Close()
	qs := &QuadStore{}
	qs.db = db
	defer qs.Close()
	err = qs.getMetadata()
	if err != errNoBucket {
		return graph.ErrDatabaseExists
	}
	err = qs.createBuckets()
	if err != nil {
		return err
	}
	err = setVersion(qs.db, latestDataVersion)
	if err != nil {
		return err
	}
	qs.Close()
	return nil
}

func newQuadStore(path string, options graph.Options) (graph.QuadStore, error) {
	var qs QuadStore
	var err error
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		clog.Errorf("Error, couldn't open! %v", err)
		return nil, err
	}
	qs.db = db
	// BoolKey returns false on non-existence. IE, Sync by default.
	qs.db.NoSync, _, err = options.BoolKey("nosync")
	if err != nil {
		return nil, err
	}
	err = qs.getMetadata()
	if err == errNoBucket {
		return nil, errors.New("bolt: quadstore has not been initialised")
	} else if err != nil {
		return nil, err
	}
	if qs.version != latestDataVersion {
		return nil, errors.New("bolt: data version is out of date. Run cayleyupgrade for your config to update the data.")
	}
	return &qs, nil
}

func (qs *QuadStore) createBuckets() error {
	return qs.db.Update(func(tx *bolt.Tx) error {
		var err error
		for _, index := range [][4]quad.Direction{spo, osp, pos, cps} {
			_, err = tx.CreateBucket(bucketFor(index))
			if err != nil {
				return fmt.Errorf("could not create bucket: %s", err)
			}
		}
		_, err = tx.CreateBucket(logBucket)
		if err != nil {
			return fmt.Errorf("could not create bucket: %s", err)
		}
		_, err = tx.CreateBucket(nodeBucket)
		if err != nil {
			return fmt.Errorf("could not create bucket: %s", err)
		}
		_, err = tx.CreateBucket(metaBucket)
		if err != nil {
			return fmt.Errorf("could not create bucket: %s", err)
		}
		return nil
	})
}

func setVersion(db *bolt.DB, version int64) error {
	return db.Update(func(tx *bolt.Tx) error {
		buf := new(bytes.Buffer)
		err := binary.Write(buf, binary.LittleEndian, version)
		if err != nil {
			clog.Errorf("Couldn't convert version!")
			return err
		}
		b := tx.Bucket(metaBucket)
		werr := b.Put([]byte("version"), buf.Bytes())
		if werr != nil {
			clog.Errorf("Couldn't write version!")
			return werr
		}
		return nil
	})
}

func (qs *QuadStore) Size() int64 {
	return qs.size
}

func (qs *QuadStore) Horizon() graph.PrimaryKey {
	return graph.NewSequentialKey(qs.horizon)
}

func (qs *QuadStore) createDeltaKeyFor(id int64) []byte {
	return []byte(fmt.Sprintf("%018x", id))
}

func bucketFor(d [4]quad.Direction) []byte {
	return []byte{d[0].Prefix(), d[1].Prefix(), d[2].Prefix(), d[3].Prefix()}
}

func (qs *QuadStore) createKeyFor(d [4]quad.Direction, q quad.Quad) []byte {
	key := make([]byte, quad.HashSize*4)
	quad.HashTo(q.Get(d[0]), key[quad.HashSize*0:quad.HashSize*1])
	quad.HashTo(q.Get(d[1]), key[quad.HashSize*1:quad.HashSize*2])
	quad.HashTo(q.Get(d[2]), key[quad.HashSize*2:quad.HashSize*3])
	quad.HashTo(q.Get(d[3]), key[quad.HashSize*3:quad.HashSize*4])
	return key
}

func (qs *QuadStore) createValueKeyFor(s quad.Value) []byte {
	return quad.HashOf(s)
}

var (
	// Short hand for direction permutations.
	spo = [4]quad.Direction{quad.Subject, quad.Predicate, quad.Object, quad.Label}
	osp = [4]quad.Direction{quad.Object, quad.Subject, quad.Predicate, quad.Label}
	pos = [4]quad.Direction{quad.Predicate, quad.Object, quad.Subject, quad.Label}
	cps = [4]quad.Direction{quad.Label, quad.Predicate, quad.Subject, quad.Object}

	// Byte arrays for each bucket name.
	spoBucket  = bucketFor(spo)
	ospBucket  = bucketFor(osp)
	posBucket  = bucketFor(pos)
	cpsBucket  = bucketFor(cps)
	logBucket  = []byte("log")
	nodeBucket = []byte("node")
	metaBucket = []byte("meta")
)

func deltaToProto(delta graph.Delta) proto.LogDelta {
	var newd proto.LogDelta
	newd.ID = uint64(delta.ID.Int())
	newd.Action = int32(delta.Action)
	newd.Timestamp = delta.Timestamp.UnixNano()
	newd.Quad = proto.MakeQuad(delta.Quad)
	return newd
}

func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta, ignoreOpts graph.IgnoreOpts) error {
	oldSize := qs.size
	oldHorizon := qs.horizon
	err := qs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(logBucket)
		b.FillPercent = localFillPercent
		resizeMap := make(map[quad.Value]int64)
		sizeChange := int64(0)
		for _, d := range deltas {
			if d.Action != graph.Add && d.Action != graph.Delete {
				return &graph.DeltaError{Delta: d, Err: graph.ErrInvalidAction}
			}
			p := deltaToProto(d)
			bytes, err := p.Marshal()
			if err != nil {
				return &graph.DeltaError{Delta: d, Err: err}
			}
			err = b.Put(qs.createDeltaKeyFor(d.ID.Int()), bytes)
			if err != nil {
				return &graph.DeltaError{Delta: d, Err: err}
			}
		}
		for _, d := range deltas {
			err := qs.buildQuadWrite(tx, d.Quad, d.ID.Int(), d.Action == graph.Add)
			if err != nil {
				if err == graph.ErrQuadExists && ignoreOpts.IgnoreDup {
					continue
				}
				if err == graph.ErrQuadNotExist && ignoreOpts.IgnoreMissing {
					continue
				}
				return &graph.DeltaError{Delta: d, Err: err}
			}
			delta := int64(1)
			if d.Action == graph.Delete {
				delta = int64(-1)
			}
			resizeMap[d.Quad.Subject] += delta
			resizeMap[d.Quad.Predicate] += delta
			resizeMap[d.Quad.Object] += delta
			if d.Quad.Label != nil {
				resizeMap[d.Quad.Label] += delta
			}
			sizeChange += delta
			qs.horizon = d.ID.Int()
		}
		for k, v := range resizeMap {
			if v != 0 {
				err := qs.UpdateValueKeyBy(k, v, tx)
				if err != nil {
					return err
				}
			}
		}
		qs.size += sizeChange
		return qs.WriteHorizonAndSize(tx)
	})

	if err != nil {
		clog.Errorf("Couldn't write to DB for Delta set. Error: %v", err)
		qs.horizon = oldHorizon
		qs.size = oldSize
	}
	return err
}

func (qs *QuadStore) buildQuadWrite(tx *bolt.Tx, q quad.Quad, id int64, isAdd bool) error {
	var entry proto.HistoryEntry
	b := tx.Bucket(spoBucket)
	b.FillPercent = localFillPercent
	data := b.Get(qs.createKeyFor(spo, q))
	if data != nil {
		// We got something.
		err := entry.Unmarshal(data)
		if err != nil {
			return err
		}
	}

	if isAdd && len(entry.History)%2 == 1 {
		clog.Errorf("attempt to add existing quad %v: %#v", entry, q)
		return graph.ErrQuadExists
	}
	if !isAdd && len(entry.History)%2 == 0 {
		clog.Errorf("attempt to delete non-existent quad %v: %#v", entry, q)
		return graph.ErrQuadNotExist
	}

	entry.History = append(entry.History, uint64(id))

	bytes, err := entry.Marshal()
	if err != nil {
		clog.Errorf("Couldn't write to buffer for entry %#v: %s", entry, err)
		return err
	}
	for _, index := range [][4]quad.Direction{spo, osp, pos, cps} {
		if index == cps && q.Get(quad.Label) == nil {
			continue
		}
		b := tx.Bucket(bucketFor(index))
		b.FillPercent = localFillPercent
		err = b.Put(qs.createKeyFor(index, q), bytes)
		if err != nil {
			return err
		}
	}
	return nil
}

func (qs *QuadStore) UpdateValueKeyBy(name quad.Value, amount int64, tx *bolt.Tx) error {
	value := proto.NodeData{
		Value: proto.MakeValue(name),
		Size:  amount,
	}
	b := tx.Bucket(nodeBucket)
	b.FillPercent = localFillPercent
	key := qs.createValueKeyFor(name)
	data := b.Get(key)

	if data != nil {
		// Node exists in the database -- unmarshal and update.
		var oldvalue proto.NodeData
		err := oldvalue.Unmarshal(data)
		if err != nil {
			clog.Errorf("Error: couldn't reconstruct value: %v", err)
			return err
		}
		oldvalue.Size += amount
		value = oldvalue
	}

	// Are we deleting something?
	if value.Size <= 0 {
		value.Size = 0
	}

	// Repackage and rewrite.
	bytes, err := value.Marshal()
	if err != nil {
		clog.Errorf("Couldn't write to buffer for value %s: %s", name, err)
		return err
	}
	err = b.Put(key, bytes)
	return err
}

func (qs *QuadStore) WriteHorizonAndSize(tx *bolt.Tx) error {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, qs.size)
	if err != nil {
		clog.Errorf("Couldn't convert size!")
		return err
	}
	b := tx.Bucket(metaBucket)
	b.FillPercent = localFillPercent
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

func (qs *QuadStore) Close() {
	qs.db.Update(func(tx *bolt.Tx) error {
		return qs.WriteHorizonAndSize(tx)
	})
	qs.db.Close()
	qs.open = false
}

func (qs *QuadStore) Quad(k graph.Value) quad.Quad {
	var d proto.LogDelta
	tok := k.(*Token)
	err := qs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(tok.bucket)
		data := b.Get(tok.key)
		if data == nil {
			return nil
		}
		var in proto.HistoryEntry
		err := in.Unmarshal(data)
		if err != nil {
			return err
		}
		if len(in.History) == 0 {
			return nil
		}
		b = tx.Bucket(logBucket)
		data = b.Get(qs.createDeltaKeyFor(int64(in.History[len(in.History)-1])))
		if data == nil {
			// No harm, no foul.
			return nil
		}
		return d.Unmarshal(data)
	})
	if err != nil {
		clog.Errorf("Error getting quad: %v", err)
		return quad.Quad{}
	}
	return d.Quad.ToNative()
}

func (qs *QuadStore) ValueOf(s quad.Value) graph.Value {
	return &Token{
		bucket: nodeBucket,
		key:    qs.createValueKeyFor(s),
	}
}

func (qs *QuadStore) valueData(t *Token) proto.NodeData {
	var out proto.NodeData
	if clog.V(3) {
		clog.Infof("%s %v", string(t.bucket), t.key)
	}
	err := qs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(t.bucket)
		data := b.Get(t.key)
		if data != nil {
			return out.Unmarshal(data)
		}
		return nil
	})
	if err != nil {
		clog.Errorf("Error: couldn't get value")
		return proto.NodeData{}
	}
	return out
}

func (qs *QuadStore) NameOf(k graph.Value) quad.Value {
	if k == nil {
		if clog.V(2) {
			clog.Infof("k was nil")
		}
		return nil
	} else if v, ok := k.(graph.PreFetchedValue); ok {
		return v.NameOf()
	}
	v := qs.valueData(k.(*Token))
	return v.GetNativeValue()
}

func (qs *QuadStore) SizeOf(k graph.Value) int64 {
	if k == nil {
		return -1
	}
	return int64(qs.valueData(k.(*Token)).Size)
}

func getInt64ForMetaKey(tx *bolt.Tx, key string, empty int64) (int64, error) {
	var out int64
	b := tx.Bucket(metaBucket)
	if b == nil {
		return empty, errNoBucket
	}
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
		qs.size, err = getInt64ForMetaKey(tx, "size", 0)
		if err != nil {
			return err
		}
		qs.version, err = getInt64ForMetaKey(tx, "version", nilDataVersion)
		if err != nil {
			return err
		}
		qs.horizon, err = getInt64ForMetaKey(tx, "horizon", 0)
		return err
	})
	return err
}

func (qs *QuadStore) QuadIterator(d quad.Direction, val graph.Value) graph.Iterator {
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

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return NewAllIterator(posBucket, quad.Predicate, qs)
}

func (qs *QuadStore) QuadDirection(val graph.Value, d quad.Direction) graph.Value {
	v := val.(*Token)
	offset := PositionOf(v, d, qs)
	if offset != -1 {
		return &Token{
			bucket: nodeBucket,
			key:    v.key[offset : offset+quad.HashSize],
		}
	}
	return qs.ValueOf(qs.Quad(v).Get(d))
}

func compareTokens(a, b graph.Value) bool {
	atok := a.(*Token)
	btok := b.(*Token)
	return bytes.Equal(atok.key, btok.key) && bytes.Equal(atok.bucket, btok.bucket)
}

func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(compareTokens)
}

func (qs *QuadStore) Type() string {
	return QuadStoreType
}
