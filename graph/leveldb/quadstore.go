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
	"encoding/binary"
	"fmt"
	"time"

	"github.com/cayleygraph/cayley/clog"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/proto"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/quad/pquads"
)

func init() {
	graph.RegisterQuadStore(QuadStoreType, graph.QuadStoreRegistration{
		NewFunc:      newQuadStore,
		UpgradeFunc:  upgradeLevelDB,
		InitFunc:     createNewLevelDB,
		IsPersistent: true,
	})
}

const (
	DefaultCacheSize       = 2
	DefaultWriteBufferSize = 20
	QuadStoreType          = "leveldb"
	horizonKey             = "__horizon"
	sizeKey                = "__size"
	versionKey             = "__version"
)

var order = binary.LittleEndian

type Token []byte

func (t Token) IsNode() bool { return len(t) > 0 && t[0] == 'z' }

func (t Token) Key() interface{} {
	return string(t)
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
	dbOpts    *opt.Options
	db        *leveldb.DB
	path      string
	open      bool
	size      int64
	horizon   int64
	writeopts *opt.WriteOptions
	readopts  *opt.ReadOptions
}

func createNewLevelDB(path string, _ graph.Options) error {
	opts := &opt.Options{}
	db, err := leveldb.OpenFile(path, opts)
	if err != nil {
		clog.Errorf("Error: could not create database: %v", err)
		return err
	}
	defer db.Close()
	qs := &QuadStore{}
	qs.db = db
	qs.writeopts = &opt.WriteOptions{
		Sync: true,
	}
	qs.readopts = &opt.ReadOptions{}
	_, err = qs.db.Get([]byte(horizonKey), qs.readopts)
	if err != nil && err != leveldb.ErrNotFound {
		clog.Errorf("couldn't read from leveldb during init")
		return err
	}
	if err != leveldb.ErrNotFound {
		return graph.ErrDatabaseExists
	}
	// Write some metadata
	if err = setVersion(qs.db, latestDataVersion, qs.writeopts); err != nil {
		clog.Errorf("couldn't write leveldb version during init")
		return err
	}
	qs.Close()
	return nil
}

func newQuadStore(path string, options graph.Options) (graph.QuadStore, error) {
	var qs QuadStore
	var err error
	qs.path = path
	cacheSize := DefaultCacheSize
	val, ok, err := options.IntKey("cache_size_mb")
	if err != nil {
		return nil, err
	} else if ok {
		cacheSize = val
	}
	qs.dbOpts = &opt.Options{
		BlockCacheCapacity: cacheSize * opt.MiB,
	}
	qs.dbOpts.ErrorIfMissing = true

	writeBufferSize := DefaultWriteBufferSize
	val, ok, err = options.IntKey("writeBufferSize")
	if err != nil {
		return nil, err
	} else if ok {
		writeBufferSize = val
	}
	qs.dbOpts.WriteBuffer = writeBufferSize * opt.MiB
	qs.writeopts = &opt.WriteOptions{
		Sync: false,
	}
	qs.readopts = &opt.ReadOptions{}
	db, err := leveldb.OpenFile(qs.path, qs.dbOpts)
	if err != nil {
		clog.Errorf("Error, could not open! %v", err)
		return nil, err
	}
	qs.db = db
	if clog.V(1) {
		clog.Infof("%v", qs.GetStats())
	}
	vers, err := getVersion(qs.db)
	if err != nil {
		clog.Errorf("Error, could not read version info! %v", err)
		db.Close()
		return nil, err
	} else if vers != latestDataVersion {
		db.Close()
		return nil, fmt.Errorf("leveldb: data version is out of date (%d vs %d). Run cayleyupgrade for your config to update the data.", vers, latestDataVersion)
	}
	err = qs.getMetadata()
	if err != nil {
		db.Close()
		return nil, err
	}
	return &qs, nil
}

func setVersion(db *leveldb.DB, version int64, wo *opt.WriteOptions) error {
	buf := make([]byte, 8)
	order.PutUint64(buf, uint64(version))
	err := db.Put([]byte(versionKey), buf, wo)
	if err != nil {
		clog.Errorf("Couldn't write version!")
		return err
	}
	return nil
}

func getVersion(db *leveldb.DB) (int64, error) {
	data, err := db.Get([]byte(versionKey), nil)
	if err == leveldb.ErrNotFound {
		return nilDataVersion, nil
	} else if len(data) != 8 {
		return 0, fmt.Errorf("version value format is unknown")
	}
	return int64(order.Uint64(data)), nil
}

func (qs *QuadStore) GetStats() string {
	out := ""
	stats, err := qs.db.GetProperty("leveldb.stats")
	if err == nil {
		out += fmt.Sprintln("Stats: ", stats)
	}
	out += fmt.Sprintln("Size: ", qs.size)
	return out
}

func (qs *QuadStore) Size() int64 {
	return qs.size
}

func (qs *QuadStore) Horizon() graph.PrimaryKey {
	return graph.NewSequentialKey(qs.horizon)
}

func createKeyFor(d [4]quad.Direction, q quad.Quad) []byte {
	key := make([]byte, 2+(quad.HashSize*4))
	key[0] = d[0].Prefix()
	key[1] = d[1].Prefix()
	quad.HashTo(q.Get(d[0]), key[2+quad.HashSize*0:2+quad.HashSize*1])
	quad.HashTo(q.Get(d[1]), key[2+quad.HashSize*1:2+quad.HashSize*2])
	quad.HashTo(q.Get(d[2]), key[2+quad.HashSize*2:2+quad.HashSize*3])
	quad.HashTo(q.Get(d[3]), key[2+quad.HashSize*3:2+quad.HashSize*4])
	return key
}

func createValueKeyFor(s quad.Value) []byte {
	key := make([]byte, 1+quad.HashSize)
	key[0] = 'z'
	quad.HashTo(s, key[1:])
	return key
}

func createDeltaKeyFor(id int64) []byte {
	key := make([]byte, 9)
	key[0] = 'd'
	order.PutUint64(key[1:], uint64(id))
	return key
}

// Short hand for direction permutations.
var (
	spo = [4]quad.Direction{quad.Subject, quad.Predicate, quad.Object, quad.Label}
	osp = [4]quad.Direction{quad.Object, quad.Subject, quad.Predicate, quad.Label}
	pos = [4]quad.Direction{quad.Predicate, quad.Object, quad.Subject, quad.Label}
	cps = [4]quad.Direction{quad.Label, quad.Predicate, quad.Subject, quad.Object}
)

func deltaToProto(delta graph.Delta, id int64, t time.Time) proto.LogDelta {
	var newd proto.LogDelta
	newd.ID = uint64(id)
	newd.Action = int32(delta.Action)
	newd.Timestamp = t.UnixNano()
	newd.Quad = pquads.MakeQuad(delta.Quad)
	return newd
}

func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta, ignoreOpts graph.IgnoreOpts) error {
	batch := &leveldb.Batch{}
	resizeMap := make(map[quad.Value]int64)
	sizeChange := int64(0)
	h, t := qs.horizon, time.Now()
	for _, d := range deltas {
		if d.Action != graph.Add && d.Action != graph.Delete {
			return &graph.DeltaError{Delta: d, Err: graph.ErrInvalidAction}
		}
		h++
		p := deltaToProto(d, h, t)
		bytes, err := p.Marshal()
		if err != nil {
			return &graph.DeltaError{Delta: d, Err: err}
		}
		batch.Put(createDeltaKeyFor(h), bytes)
		err = qs.buildQuadWrite(batch, d.Quad, h, d.Action == graph.Add)
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
		qs.horizon = h
	}
	for k, v := range resizeMap {
		if v != 0 {
			err := qs.UpdateValueKeyBy(k, v, batch)
			if err != nil {
				return err
			}
		}
	}
	err := qs.db.Write(batch, qs.writeopts)
	if err != nil {
		clog.Errorf("could not write to DB for quadset.")
		return err
	}
	qs.size += sizeChange
	return nil
}

func (qs *QuadStore) buildQuadWrite(batch *leveldb.Batch, q quad.Quad, id int64, isAdd bool) error {
	var entry proto.HistoryEntry
	data, err := qs.db.Get(createKeyFor(spo, q), qs.readopts)
	if err != nil && err != leveldb.ErrNotFound {
		clog.Errorf("could not access DB to prepare index: %v", err)
		return err
	}
	if data != nil {
		// We got something.
		err = entry.Unmarshal(data)
		if err != nil {
			return err
		}
	}

	if isAdd && len(entry.History)%2 == 1 {
		return graph.ErrQuadExists
	}
	if !isAdd && len(entry.History)%2 == 0 {
		return graph.ErrQuadNotExist
	}

	entry.History = append(entry.History, uint64(id))

	bytes, err := entry.Marshal()
	if err != nil {
		clog.Errorf("could not write to buffer for entry %#v: %s", entry, err)
		return err
	}
	batch.Put(createKeyFor(spo, q), bytes)
	batch.Put(createKeyFor(osp, q), bytes)
	batch.Put(createKeyFor(pos, q), bytes)
	if q.Get(quad.Label) != nil {
		batch.Put(createKeyFor(cps, q), bytes)
	}
	return nil
}

func (qs *QuadStore) UpdateValueKeyBy(name quad.Value, amount int64, batch *leveldb.Batch) error {
	value := proto.NodeData{
		Value: pquads.MakeValue(name),
		Size:  amount,
	}
	key := createValueKeyFor(name)
	b, err := qs.db.Get(key, qs.readopts)

	// Error getting the node from the database.
	if err != nil && err != leveldb.ErrNotFound {
		clog.Errorf("Error reading Value %s from the DB.", name)
		return err
	}

	// Node exists in the database -- unmarshal and update.
	if b != nil && err != leveldb.ErrNotFound {
		var oldvalue proto.NodeData
		err = oldvalue.Unmarshal(b)
		if err != nil {
			clog.Errorf("Error: could not reconstruct value: %v", err)
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
		clog.Errorf("could not write to buffer for value %s: %s", name, err)
		return err
	}
	if batch == nil {
		qs.db.Put(key, bytes, qs.writeopts)
	} else {
		batch.Put(key, bytes)
	}
	return nil
}

func (qs *QuadStore) Close() error {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, order, qs.size)
	if err == nil {
		werr := qs.db.Put([]byte(sizeKey), buf.Bytes(), qs.writeopts)
		if werr != nil {
			clog.Errorf("could not write size before closing!")
		}
	} else {
		clog.Errorf("could not convert size before closing!")
	}
	buf.Reset()
	err = binary.Write(buf, order, qs.horizon)
	if err == nil {
		werr := qs.db.Put([]byte(horizonKey), buf.Bytes(), qs.writeopts)
		if werr != nil {
			clog.Errorf("could not write horizon before closing!")
		}
	} else {
		clog.Errorf("could not convert horizon before closing!")
	}
	err = qs.db.Close()
	qs.open = false
	return err
}

func (qs *QuadStore) Quad(k graph.Value) quad.Quad {
	var in proto.HistoryEntry
	b, err := qs.db.Get(k.(Token), qs.readopts)
	if err == leveldb.ErrNotFound {
		// No harm, no foul.
		return quad.Quad{}
	} else if err != nil {
		clog.Errorf("Error: could not get quad from DB. %v", err)
		return quad.Quad{}
	}
	err = in.Unmarshal(b)
	if err != nil {
		clog.Errorf("Error: could not reconstruct history. %v", err)
		return quad.Quad{}
	}
	b, err = qs.db.Get(createDeltaKeyFor(int64(in.History[len(in.History)-1])), qs.readopts)
	if err == leveldb.ErrNotFound {
		// No harm, no foul.
		return quad.Quad{}
	} else if err != nil {
		clog.Errorf("Error: could not get quad from DB. %v", err)
		return quad.Quad{}
	}
	var d proto.LogDelta
	err = d.Unmarshal(b)
	if err != nil {
		clog.Errorf("Error: could not reconstruct quad. %v", err)
		return quad.Quad{}
	}
	return d.Quad.ToNative()
}

func (qs *QuadStore) ValueOf(s quad.Value) graph.Value {
	return Token(createValueKeyFor(s))
}

func (qs *QuadStore) valueData(key []byte) proto.NodeData {
	var out proto.NodeData
	if clog.V(3) {
		clog.Infof("%c %v", key[0], key)
	}
	b, err := qs.db.Get(key, qs.readopts)
	if err != nil && err != leveldb.ErrNotFound {
		clog.Errorf("Error: could not get value from DB")
		return out
	}
	if b != nil && err != leveldb.ErrNotFound {
		err = out.Unmarshal(b)
		if err != nil {
			clog.Errorf("Error: could not reconstruct value: %v", err)
			return proto.NodeData{}
		}
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
	v := qs.valueData(k.(Token))
	return v.GetNativeValue()
}

func (qs *QuadStore) SizeOf(k graph.Value) int64 {
	if k == nil {
		return 0
	}
	return int64(qs.valueData(k.(Token)).Size)
}

func (qs *QuadStore) getInt64ForKey(key string, empty int64) (int64, error) {
	var out int64
	b, err := qs.db.Get([]byte(key), qs.readopts)
	if err != nil && err != leveldb.ErrNotFound {
		clog.Errorf("could not read %v: %v", key, err)
		return 0, err
	}
	if err == leveldb.ErrNotFound {
		// Must be a new database. Cool
		return empty, nil
	}
	buf := bytes.NewBuffer(b)
	err = binary.Read(buf, order, &out)
	if err != nil {
		clog.Errorf("Error: could not parse %v", key)
		return 0, err
	}
	return out, nil
}

func (qs *QuadStore) getMetadata() error {
	var err error
	qs.size, err = qs.getInt64ForKey(sizeKey, 0)
	if err != nil {
		return err
	}
	qs.horizon, err = qs.getInt64ForKey(horizonKey, 0)
	return err
}

func (qs *QuadStore) SizeOfPrefix(pre []byte) (int64, error) {
	limit := make([]byte, len(pre))
	copy(limit, pre)
	end := len(limit) - 1
	limit[end]++
	ranges := make([]util.Range, 1)
	ranges[0].Start = pre
	ranges[0].Limit = limit
	sizes, err := qs.db.SizeOf(ranges)
	if err == nil {
		return (int64(sizes[0]) >> 6) + 1, nil
	}
	return 0, nil
}

func (qs *QuadStore) QuadIterator(d quad.Direction, val graph.Value) graph.Iterator {
	var prefix string
	switch d {
	case quad.Subject:
		prefix = "sp"
	case quad.Predicate:
		prefix = "po"
	case quad.Object:
		prefix = "os"
	case quad.Label:
		prefix = "cp"
	default:
		panic("unreachable " + d.String())
	}
	return NewIterator(prefix, d, val, qs)
}

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator("z", quad.Any, qs)
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return NewAllIterator("po", quad.Predicate, qs)
}

func (qs *QuadStore) QuadDirection(val graph.Value, d quad.Direction) graph.Value {
	v := val.(Token)
	offset := PositionOf(v[0:2], d, qs)
	return Token(append([]byte("z"), v[offset:offset+quad.HashSize]...))
}

func compareBytes(a, b graph.Value) bool {
	return bytes.Equal(a.(Token), b.(Token))
}

func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(compareBytes)
}
