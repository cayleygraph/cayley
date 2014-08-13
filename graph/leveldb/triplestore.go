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
	"github.com/google/cayley/quad"
)

func init() {
	graph.RegisterTripleStore("leveldb", true, newTripleStore, createNewLevelDB)
}

const (
	DefaultCacheSize       = 2
	DefaultWriteBufferSize = 20
)

type Token []byte

func (t Token) Key() interface{} {
	return string(t)
}

type TripleStore struct {
	dbOpts     *opt.Options
	db         *leveldb.DB
	path       string
	open       bool
	size       int64
	hasherSize int
	makeHasher func() hash.Hash
	writeopts  *opt.WriteOptions
	readopts   *opt.ReadOptions
}

func createNewLevelDB(path string, _ graph.Options) error {
	opts := &opt.Options{}
	db, err := leveldb.OpenFile(path, opts)
	if err != nil {
		glog.Errorf("Error: couldn't create database: %v", err)
		return err
	}
	defer db.Close()
	qs := &TripleStore{}
	qs.db = db
	qs.writeopts = &opt.WriteOptions{
		Sync: true,
	}
	qs.Close()
	return nil
}

func newTripleStore(path string, options graph.Options) (graph.TripleStore, error) {
	var qs TripleStore
	qs.path = path
	cache_size := DefaultCacheSize
	if val, ok := options.IntKey("cache_size_mb"); ok {
		cache_size = val
	}
	qs.dbOpts = &opt.Options{
		BlockCache: cache.NewLRUCache(cache_size * opt.MiB),
	}
	qs.dbOpts.ErrorIfMissing = true

	write_buffer_mb := DefaultWriteBufferSize
	if val, ok := options.IntKey("write_buffer_mb"); ok {
		write_buffer_mb = val
	}
	qs.dbOpts.WriteBuffer = write_buffer_mb * opt.MiB
	qs.hasherSize = sha1.Size
	qs.makeHasher = sha1.New
	qs.writeopts = &opt.WriteOptions{
		Sync: false,
	}
	qs.readopts = &opt.ReadOptions{}
	db, err := leveldb.OpenFile(qs.path, qs.dbOpts)
	if err != nil {
		panic("Error, couldn't open! " + err.Error())
	}
	qs.db = db
	glog.Infoln(qs.GetStats())
	qs.getSize()
	return &qs, nil
}

func (qs *TripleStore) GetStats() string {
	out := ""
	stats, err := qs.db.GetProperty("leveldb.stats")
	if err == nil {
		out += fmt.Sprintln("Stats: ", stats)
	}
	out += fmt.Sprintln("Size: ", qs.size)
	return out
}

func (qs *TripleStore) Size() int64 {
	return qs.size
}

func (qs *TripleStore) createKeyFor(d [3]quad.Direction, triple quad.Quad) []byte {
	hasher := qs.makeHasher()
	key := make([]byte, 0, 2+(qs.hasherSize*3))
	// TODO(kortschak) Remove dependence on String() method.
	key = append(key, []byte{d[0].Prefix(), d[1].Prefix()}...)
	key = append(key, qs.convertStringToByteHash(triple.Get(d[0]), hasher)...)
	key = append(key, qs.convertStringToByteHash(triple.Get(d[1]), hasher)...)
	key = append(key, qs.convertStringToByteHash(triple.Get(d[2]), hasher)...)
	return key
}

func (qs *TripleStore) createProvKeyFor(d [3]quad.Direction, triple quad.Quad) []byte {
	hasher := qs.makeHasher()
	key := make([]byte, 0, 2+(qs.hasherSize*4))
	// TODO(kortschak) Remove dependence on String() method.
	key = append(key, []byte{quad.Label.Prefix(), d[0].Prefix()}...)
	key = append(key, qs.convertStringToByteHash(triple.Get(quad.Label), hasher)...)
	key = append(key, qs.convertStringToByteHash(triple.Get(d[0]), hasher)...)
	key = append(key, qs.convertStringToByteHash(triple.Get(d[1]), hasher)...)
	key = append(key, qs.convertStringToByteHash(triple.Get(d[2]), hasher)...)
	return key
}

func (qs *TripleStore) createValueKeyFor(s string) []byte {
	hasher := qs.makeHasher()
	key := make([]byte, 0, 1+qs.hasherSize)
	key = append(key, []byte("z")...)
	key = append(key, qs.convertStringToByteHash(s, hasher)...)
	return key
}

func (qs *TripleStore) AddTriple(t quad.Quad) {
	batch := &leveldb.Batch{}
	qs.buildWrite(batch, t)
	err := qs.db.Write(batch, qs.writeopts)
	if err != nil {
		glog.Errorf("Couldn't write to DB for triple %s.", t)
		return
	}
	qs.size++
}

// Short hand for direction permutations.
var (
	spo = [3]quad.Direction{quad.Subject, quad.Predicate, quad.Object}
	osp = [3]quad.Direction{quad.Object, quad.Subject, quad.Predicate}
	pos = [3]quad.Direction{quad.Predicate, quad.Object, quad.Subject}
	pso = [3]quad.Direction{quad.Predicate, quad.Subject, quad.Object}
)

func (qs *TripleStore) RemoveTriple(t quad.Quad) {
	_, err := qs.db.Get(qs.createKeyFor(spo, t), qs.readopts)
	if err != nil && err != leveldb.ErrNotFound {
		glog.Error("Couldn't access DB to confirm deletion")
		return
	}
	if err == leveldb.ErrNotFound {
		// No such triple in the database, forget about it.
		return
	}
	batch := &leveldb.Batch{}
	batch.Delete(qs.createKeyFor(spo, t))
	batch.Delete(qs.createKeyFor(osp, t))
	batch.Delete(qs.createKeyFor(pos, t))
	qs.UpdateValueKeyBy(t.Get(quad.Subject), -1, batch)
	qs.UpdateValueKeyBy(t.Get(quad.Predicate), -1, batch)
	qs.UpdateValueKeyBy(t.Get(quad.Object), -1, batch)
	if t.Get(quad.Label) != "" {
		batch.Delete(qs.createProvKeyFor(pso, t))
		qs.UpdateValueKeyBy(t.Get(quad.Label), -1, batch)
	}
	err = qs.db.Write(batch, nil)
	if err != nil {
		glog.Errorf("Couldn't delete triple %s.", t)
		return
	}
	qs.size--
}

func (qs *TripleStore) buildTripleWrite(batch *leveldb.Batch, t quad.Quad) {
	bytes, err := json.Marshal(t)
	if err != nil {
		glog.Errorf("Couldn't write to buffer for triple %s: %s", t, err)
		return
	}
	batch.Put(qs.createKeyFor(spo, t), bytes)
	batch.Put(qs.createKeyFor(osp, t), bytes)
	batch.Put(qs.createKeyFor(pos, t), bytes)
	if t.Get(quad.Label) != "" {
		batch.Put(qs.createProvKeyFor(pso, t), bytes)
	}
}

func (qs *TripleStore) buildWrite(batch *leveldb.Batch, t quad.Quad) {
	qs.buildTripleWrite(batch, t)
	qs.UpdateValueKeyBy(t.Get(quad.Subject), 1, nil)
	qs.UpdateValueKeyBy(t.Get(quad.Predicate), 1, nil)
	qs.UpdateValueKeyBy(t.Get(quad.Object), 1, nil)
	if t.Get(quad.Label) != "" {
		qs.UpdateValueKeyBy(t.Get(quad.Label), 1, nil)
	}
}

type ValueData struct {
	Name string
	Size int64
}

func (qs *TripleStore) UpdateValueKeyBy(name string, amount int, batch *leveldb.Batch) {
	value := &ValueData{name, int64(amount)}
	key := qs.createValueKeyFor(name)
	b, err := qs.db.Get(key, qs.readopts)

	// Error getting the node from the database.
	if err != nil && err != leveldb.ErrNotFound {
		glog.Errorf("Error reading Value %s from the DB.", name)
		return
	}

	// Node exists in the database -- unmarshal and update.
	if b != nil && err != leveldb.ErrNotFound {
		err = json.Unmarshal(b, value)
		if err != nil {
			glog.Errorf("Error: couldn't reconstruct value: %v", err)
			return
		}
		value.Size += int64(amount)
	}

	// Are we deleting something?
	if amount < 0 {
		if value.Size <= 0 {
			if batch == nil {
				qs.db.Delete(key, qs.writeopts)
			} else {
				batch.Delete(key)
			}
			return
		}
	}

	// Repackage and rewrite.
	bytes, err := json.Marshal(&value)
	if err != nil {
		glog.Errorf("Couldn't write to buffer for value %s: %s", name, err)
		return
	}
	if batch == nil {
		qs.db.Put(key, bytes, qs.writeopts)
	} else {
		batch.Put(key, bytes)
	}
}

func (qs *TripleStore) AddTripleSet(t_s []quad.Quad) {
	batch := &leveldb.Batch{}
	newTs := len(t_s)
	resizeMap := make(map[string]int)
	for _, t := range t_s {
		qs.buildTripleWrite(batch, t)
		resizeMap[t.Subject]++
		resizeMap[t.Predicate]++
		resizeMap[t.Object]++
		if t.Label != "" {
			resizeMap[t.Label]++
		}
	}
	for k, v := range resizeMap {
		qs.UpdateValueKeyBy(k, v, batch)
	}
	err := qs.db.Write(batch, qs.writeopts)
	if err != nil {
		glog.Error("Couldn't write to DB for tripleset.")
		return
	}
	qs.size += int64(newTs)
}

func (qs *TripleStore) Close() {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, qs.size)
	if err == nil {
		werr := qs.db.Put([]byte("__size"), buf.Bytes(), qs.writeopts)
		if werr != nil {
			glog.Error("Couldn't write size before closing!")
		}
	} else {
		glog.Errorf("Couldn't convert size before closing!")
	}
	qs.db.Close()
	qs.open = false
}

func (qs *TripleStore) Quad(k graph.Value) quad.Quad {
	var triple quad.Quad
	b, err := qs.db.Get(k.(Token), qs.readopts)
	if err != nil && err != leveldb.ErrNotFound {
		glog.Error("Error: couldn't get triple from DB.")
		return quad.Quad{}
	}
	if err == leveldb.ErrNotFound {
		// No harm, no foul.
		return quad.Quad{}
	}
	err = json.Unmarshal(b, &triple)
	if err != nil {
		glog.Error("Error: couldn't reconstruct triple.")
		return quad.Quad{}
	}
	return triple
}

func (qs *TripleStore) convertStringToByteHash(s string, hasher hash.Hash) []byte {
	hasher.Reset()
	key := make([]byte, 0, qs.hasherSize)
	hasher.Write([]byte(s))
	key = hasher.Sum(key)
	return key
}

func (qs *TripleStore) ValueOf(s string) graph.Value {
	return Token(qs.createValueKeyFor(s))
}

func (qs *TripleStore) valueData(value_key []byte) ValueData {
	var out ValueData
	if glog.V(3) {
		glog.V(3).Infof("%s %v", string(value_key[0]), value_key)
	}
	b, err := qs.db.Get(value_key, qs.readopts)
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

func (qs *TripleStore) NameOf(k graph.Value) string {
	if k == nil {
		glog.V(2).Info("k was nil")
		return ""
	}
	return qs.valueData(k.(Token)).Name
}

func (qs *TripleStore) SizeOf(k graph.Value) int64 {
	if k == nil {
		return 0
	}
	return int64(qs.valueData(k.(Token)).Size)
}

func (qs *TripleStore) getSize() {
	var size int64
	b, err := qs.db.Get([]byte("__size"), qs.readopts)
	if err != nil && err != leveldb.ErrNotFound {
		panic("Couldn't read size " + err.Error())
	}
	if err == leveldb.ErrNotFound {
		// Must be a new database. Cool
		qs.size = 0
		return
	}
	buf := bytes.NewBuffer(b)
	err = binary.Read(buf, binary.LittleEndian, &size)
	if err != nil {
		glog.Errorln("Error: couldn't parse size")
	}
	qs.size = size
}

func (qs *TripleStore) SizeOfPrefix(pre []byte) (int64, error) {
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

func (qs *TripleStore) TripleIterator(d quad.Direction, val graph.Value) graph.Iterator {
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

func (qs *TripleStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator("z", quad.Any, qs)
}

func (qs *TripleStore) TriplesAllIterator() graph.Iterator {
	return NewAllIterator("po", quad.Predicate, qs)
}

func (qs *TripleStore) TripleDirection(val graph.Value, d quad.Direction) graph.Value {
	v := val.(Token)
	offset := PositionOf(v[0:2], d, qs)
	if offset != -1 {
		return Token(append([]byte("z"), v[offset:offset+qs.hasherSize]...))
	} else {
		return Token(qs.Quad(val).Get(d))
	}
}

func compareBytes(a, b graph.Value) bool {
	return bytes.Equal(a.(Token), b.(Token))
}

func (qs *TripleStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixedIteratorWithCompare(compareBytes)
}
