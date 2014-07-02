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
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"

	"github.com/barakmich/glog"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/cache"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
)

const (
	DefaultCacheSize       = 2
	DefaultWriteBufferSize = 20
)

type TripleStore struct {
	dbOpts    *opt.Options
	db        *leveldb.DB
	path      string
	open      bool
	size      int64
	hasher    hash.Hash
	writeopts *opt.WriteOptions
	readopts  *opt.ReadOptions
}

func CreateNewLevelDB(path string) bool {
	opts := &opt.Options{}
	db, err := leveldb.OpenFile(path, opts)
	if err != nil {
		glog.Errorln("Error: couldn't create database: ", err)
		return false
	}
	defer db.Close()
	ts := &TripleStore{}
	ts.db = db
	ts.writeopts = &opt.WriteOptions{
		Sync: true,
	}
	ts.Close()
	return true
}

func NewTripleStore(path string, options graph.Options) *TripleStore {
	var ts TripleStore
	ts.path = path
	cache_size := DefaultCacheSize
	if val, ok := options.IntKey("cache_size_mb"); ok {
		cache_size = val
	}
	ts.dbOpts = &opt.Options{
		BlockCache: cache.NewLRUCache(cache_size * opt.MiB),
	}
	ts.dbOpts.ErrorIfMissing = true

	write_buffer_mb := DefaultWriteBufferSize
	if val, ok := options.IntKey("write_buffer_mb"); ok {
		write_buffer_mb = val
	}
	ts.dbOpts.WriteBuffer = write_buffer_mb * opt.MiB
	ts.hasher = sha1.New()
	ts.writeopts = &opt.WriteOptions{
		Sync: false,
	}
	ts.readopts = &opt.ReadOptions{}
	db, err := leveldb.OpenFile(ts.path, ts.dbOpts)
	if err != nil {
		panic("Error, couldn't open! " + err.Error())
	}
	ts.db = db
	glog.Infoln(ts.GetStats())
	ts.getSize()
	return &ts
}

func (ts *TripleStore) GetStats() string {
	out := ""
	stats, err := ts.db.GetProperty("leveldb.stats")
	if err == nil {
		out += fmt.Sprintln("Stats: ", stats)
	}
	out += fmt.Sprintln("Size: ", ts.size)
	return out
}

func (ts *TripleStore) Size() int64 {
	return ts.size
}

func (ts *TripleStore) createKeyFor(d [3]graph.Direction, triple *graph.Triple) []byte {
	key := make([]byte, 0, 2+(ts.hasher.Size()*3))
	// TODO(kortschak) Remove dependence on String() method.
	key = append(key, []byte(d[0].String()+d[1].String())...)
	key = append(key, ts.convertStringToByteHash(triple.Get(d[0]))...)
	key = append(key, ts.convertStringToByteHash(triple.Get(d[1]))...)
	key = append(key, ts.convertStringToByteHash(triple.Get(d[2]))...)
	return key
}

func (ts *TripleStore) createProvKeyFor(d [3]graph.Direction, triple *graph.Triple) []byte {
	key := make([]byte, 0, 2+(ts.hasher.Size()*4))
	// TODO(kortschak) Remove dependence on String() method.
	key = append(key, []byte(graph.Provenance.String()+d[0].String())...)
	key = append(key, ts.convertStringToByteHash(triple.Get(graph.Provenance))...)
	key = append(key, ts.convertStringToByteHash(triple.Get(d[0]))...)
	key = append(key, ts.convertStringToByteHash(triple.Get(d[1]))...)
	key = append(key, ts.convertStringToByteHash(triple.Get(d[2]))...)
	return key
}

func (ts *TripleStore) createValueKeyFor(s string) []byte {
	key := make([]byte, 0, 1+ts.hasher.Size())
	key = append(key, []byte("z")...)
	key = append(key, ts.convertStringToByteHash(s)...)
	return key
}

func (ts *TripleStore) AddTriple(t *graph.Triple) {
	batch := &leveldb.Batch{}
	ts.buildWrite(batch, t)
	err := ts.db.Write(batch, ts.writeopts)
	if err != nil {
		glog.Errorf("Couldn't write to DB for triple %s", t)
		return
	}
	ts.size++
}

// Short hand for direction permutations.
var (
	spo = [3]graph.Direction{graph.Subject, graph.Predicate, graph.Object}
	osp = [3]graph.Direction{graph.Object, graph.Subject, graph.Predicate}
	pos = [3]graph.Direction{graph.Predicate, graph.Object, graph.Subject}
	pso = [3]graph.Direction{graph.Predicate, graph.Subject, graph.Object}
)

func (ts *TripleStore) RemoveTriple(t *graph.Triple) {
	_, err := ts.db.Get(ts.createKeyFor(spo, t), ts.readopts)
	if err != nil && err != leveldb.ErrNotFound {
		glog.Errorf("Couldn't access DB to confirm deletion")
		return
	}
	if err == leveldb.ErrNotFound {
		// No such triple in the database, forget about it.
		return
	}
	batch := &leveldb.Batch{}
	batch.Delete(ts.createKeyFor(spo, t))
	batch.Delete(ts.createKeyFor(osp, t))
	batch.Delete(ts.createKeyFor(pos, t))
	ts.UpdateValueKeyBy(t.Get(graph.Subject), -1, batch)
	ts.UpdateValueKeyBy(t.Get(graph.Predicate), -1, batch)
	ts.UpdateValueKeyBy(t.Get(graph.Object), -1, batch)
	if t.Get(graph.Provenance) != "" {
		batch.Delete(ts.createProvKeyFor(pso, t))
		ts.UpdateValueKeyBy(t.Get(graph.Provenance), -1, batch)
	}
	err = ts.db.Write(batch, nil)
	if err != nil {
		glog.Errorf("Couldn't delete triple %s", t)
		return
	}
	ts.size--
}

func (ts *TripleStore) buildTripleWrite(batch *leveldb.Batch, t *graph.Triple) {
	bytes, err := json.Marshal(*t)
	if err != nil {
		glog.Errorf("Couldn't write to buffer for triple %s\n  %s\n", t, err)
		return
	}
	batch.Put(ts.createKeyFor(spo, t), bytes)
	batch.Put(ts.createKeyFor(osp, t), bytes)
	batch.Put(ts.createKeyFor(pos, t), bytes)
	if t.Get(graph.Provenance) != "" {
		batch.Put(ts.createProvKeyFor(pso, t), bytes)
	}
}

func (ts *TripleStore) buildWrite(batch *leveldb.Batch, t *graph.Triple) {
	ts.buildTripleWrite(batch, t)
	ts.UpdateValueKeyBy(t.Get(graph.Subject), 1, nil)
	ts.UpdateValueKeyBy(t.Get(graph.Predicate), 1, nil)
	ts.UpdateValueKeyBy(t.Get(graph.Object), 1, nil)
	if t.Get(graph.Provenance) != "" {
		ts.UpdateValueKeyBy(t.Get(graph.Provenance), 1, nil)
	}
}

type ValueData struct {
	Name string
	Size int64
}

func (ts *TripleStore) UpdateValueKeyBy(name string, amount int, batch *leveldb.Batch) {
	value := &ValueData{name, int64(amount)}
	key := ts.createValueKeyFor(name)
	b, err := ts.db.Get(key, ts.readopts)

	// Error getting the node from the database.
	if err != nil && err != leveldb.ErrNotFound {
		glog.Errorf("Error reading Value %s from the DB\n", name)
		return
	}

	// Node exists in the database -- unmarshal and update.
	if b != nil && err != leveldb.ErrNotFound {
		err = json.Unmarshal(b, value)
		if err != nil {
			glog.Errorln("Error: couldn't reconstruct value ", err)
			return
		}
		value.Size += int64(amount)
	}

	// Are we deleting something?
	if amount < 0 {
		if value.Size <= 0 {
			if batch == nil {
				ts.db.Delete(key, ts.writeopts)
			} else {
				batch.Delete(key)
			}
			return
		}
	}

	// Repackage and rewrite.
	bytes, err := json.Marshal(&value)
	if err != nil {
		glog.Errorf("Couldn't write to buffer for value %s\n %s", name, err)
		return
	}
	if batch == nil {
		ts.db.Put(key, bytes, ts.writeopts)
	} else {
		batch.Put(key, bytes)
	}
}

func (ts *TripleStore) AddTripleSet(t_s []*graph.Triple) {
	batch := &leveldb.Batch{}
	newTs := len(t_s)
	resizeMap := make(map[string]int)
	for _, t := range t_s {
		ts.buildTripleWrite(batch, t)
		resizeMap[t.Subject]++
		resizeMap[t.Predicate]++
		resizeMap[t.Object]++
		if t.Provenance != "" {
			resizeMap[t.Provenance]++
		}
	}
	for k, v := range resizeMap {
		ts.UpdateValueKeyBy(k, v, batch)
	}
	err := ts.db.Write(batch, ts.writeopts)
	if err != nil {
		glog.Errorf("Couldn't write to DB for tripleset")
		return
	}
	ts.size += int64(newTs)
}

func (ts *TripleStore) Close() {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, ts.size)
	if err == nil {
		werr := ts.db.Put([]byte("__size"), buf.Bytes(), ts.writeopts)
		if werr != nil {
			glog.Errorf("Couldn't write size before closing!")
		}
	} else {
		glog.Errorf("Couldn't convert size before closing!")
	}
	ts.db.Close()
	ts.open = false
}

func (ts *TripleStore) Triple(k graph.TSVal) *graph.Triple {
	var triple graph.Triple
	b, err := ts.db.Get(k.([]byte), ts.readopts)
	if err != nil && err != leveldb.ErrNotFound {
		glog.Errorln("Error: couldn't get triple from DB")
		return &graph.Triple{}
	}
	if err == leveldb.ErrNotFound {
		// No harm, no foul.
		return &graph.Triple{}
	}
	err = json.Unmarshal(b, &triple)
	if err != nil {
		glog.Errorln("Error: couldn't reconstruct triple")
		return &graph.Triple{}
	}
	return &triple
}

func (ts *TripleStore) convertStringToByteHash(s string) []byte {
	ts.hasher.Reset()
	key := make([]byte, 0, ts.hasher.Size())
	ts.hasher.Write([]byte(s))
	key = ts.hasher.Sum(key)
	return key
}

func (ts *TripleStore) ValueOf(s string) graph.TSVal {
	return ts.createValueKeyFor(s)
}

func (ts *TripleStore) getValueData(value_key []byte) ValueData {
	var out ValueData
	if glog.V(3) {
		glog.V(3).Infof("%s %v\n", string(value_key[0]), value_key)
	}
	b, err := ts.db.Get(value_key, ts.readopts)
	if err != nil && err != leveldb.ErrNotFound {
		glog.Errorln("Error: couldn't get value from DB")
		return out
	}
	if b != nil && err != leveldb.ErrNotFound {
		err = json.Unmarshal(b, &out)
		if err != nil {
			glog.Errorln("Error: couldn't reconstruct value")
			return ValueData{}
		}
	}
	return out
}

func (ts *TripleStore) NameOf(k graph.TSVal) string {
	if k == nil {
		glog.V(2).Infoln("k was nil")
		return ""
	}
	return ts.getValueData(k.([]byte)).Name
}

func (ts *TripleStore) GetSizeFor(k graph.TSVal) int64 {
	if k == nil {
		return 0
	}
	return int64(ts.getValueData(k.([]byte)).Size)
}

func (ts *TripleStore) getSize() {
	var size int64
	b, err := ts.db.Get([]byte("__size"), ts.readopts)
	if err != nil && err != leveldb.ErrNotFound {
		panic("Couldn't read size " + err.Error())
	}
	if err == leveldb.ErrNotFound {
		// Must be a new database. Cool
		ts.size = 0
		return
	}
	buf := bytes.NewBuffer(b)
	err = binary.Read(buf, binary.LittleEndian, &size)
	if err != nil {
		glog.Errorln("Error: couldn't parse size")
	}
	ts.size = size
}

func (ts *TripleStore) GetApproximateSizeForPrefix(pre []byte) (int64, error) {
	limit := make([]byte, len(pre))
	copy(limit, pre)
	end := len(limit) - 1
	limit[end]++
	ranges := make([]util.Range, 1)
	ranges[0].Start = pre
	ranges[0].Limit = limit
	sizes, err := ts.db.GetApproximateSizes(ranges)
	if err == nil {
		return (int64(sizes[0]) >> 6) + 1, nil
	}
	return 0, nil
}

func (ts *TripleStore) TripleIterator(d graph.Direction, val graph.TSVal) graph.Iterator {
	var prefix string
	switch d {
	case graph.Subject:
		prefix = "sp"
	case graph.Predicate:
		prefix = "po"
	case graph.Object:
		prefix = "os"
	case graph.Provenance:
		prefix = "cp"
	default:
		panic("unreachable " + d.String())
	}
	return NewIterator(prefix, d, val, ts)
}

func (ts *TripleStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator("z", graph.Any, ts)
}

func (ts *TripleStore) TriplesAllIterator() graph.Iterator {
	return NewAllIterator("po", graph.Predicate, ts)
}

func (ts *TripleStore) TripleDirection(val graph.TSVal, d graph.Direction) graph.TSVal {
	v := val.([]uint8)
	offset := GetPositionFromPrefix(v[0:2], d, ts)
	if offset != -1 {
		return append([]byte("z"), v[offset:offset+ts.hasher.Size()]...)
	} else {
		return ts.Triple(val).Get(d)
	}
}

func compareBytes(a, b graph.TSVal) bool {
	return bytes.Equal(a.([]uint8), b.([]uint8))
}

func (ts *TripleStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixedIteratorWithCompare(compareBytes)
}
