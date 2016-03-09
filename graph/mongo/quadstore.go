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

package mongo

import (
	"encoding/hex"
	"fmt"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/proto"
	"github.com/cayleygraph/cayley/quad"
)

const DefaultDBName = "cayley"
const QuadStoreType = "mongo"

func init() {
	graph.RegisterQuadStore(QuadStoreType, graph.QuadStoreRegistration{
		NewFunc:           newQuadStore,
		NewForRequestFunc: nil,
		UpgradeFunc:       nil,
		InitFunc:          createNewMongoGraph,
		IsPersistent:      true,
	})
}

type QuadStore struct {
	session *mgo.Session
	db      *mgo.Database
	ids     *cache
	sizes   *cache
}

func createNewMongoGraph(addr string, options graph.Options) error {
	conn, err := mgo.Dial(addr)
	if err != nil {
		return err
	}
	conn.SetSafe(&mgo.Safe{})
	dbName := DefaultDBName
	val, ok, err := options.StringKey("database_name")
	if err != nil {
		return err
	} else if ok {
		dbName = val
	}
	db := conn.DB(dbName)
	indexOpts := mgo.Index{
		Key:        []string{"subject"},
		Unique:     false,
		DropDups:   false,
		Background: true,
		Sparse:     true,
	}
	db.C("quads").EnsureIndex(indexOpts)
	indexOpts.Key = []string{"predicate"}
	db.C("quads").EnsureIndex(indexOpts)
	indexOpts.Key = []string{"object"}
	db.C("quads").EnsureIndex(indexOpts)
	indexOpts.Key = []string{"label"}
	db.C("quads").EnsureIndex(indexOpts)
	logOpts := mgo.Index{
		Key:        []string{"LogID"},
		Unique:     true,
		DropDups:   false,
		Background: true,
		Sparse:     true,
	}
	db.C("log").EnsureIndex(logOpts)
	return nil
}

func newQuadStore(addr string, options graph.Options) (graph.QuadStore, error) {
	var qs QuadStore
	conn, err := mgo.Dial(addr)
	if err != nil {
		return nil, err
	}
	conn.SetSafe(&mgo.Safe{})
	dbName := DefaultDBName
	val, ok, err := options.StringKey("database_name")
	if err != nil {
		return nil, err
	} else if ok {
		dbName = val
	}
	qs.db = conn.DB(dbName)
	qs.session = conn
	qs.ids = newCache(1 << 16)
	qs.sizes = newCache(1 << 16)
	return &qs, nil
}

func (qs *QuadStore) getIDForQuad(t quad.Quad) string {
	id := hashOf(t.Subject)
	id += hashOf(t.Predicate)
	id += hashOf(t.Object)
	id += hashOf(t.Label)
	return id
}

func hashOf(s quad.Value) string {
	return hex.EncodeToString(quad.HashOf(s))
}

type MongoNode struct {
	ID   string `bson:"_id"`
	Name value  `bson:"Name"`
	Size int    `bson:"Size"`
}

type MongoLogEntry struct {
	LogID     int64  `bson:"LogID"`
	Action    string `bson:"Action"`
	Key       string `bson:"Key"`
	Timestamp int64
}

func (qs *QuadStore) updateNodeBy(name quad.Value, inc int) error {
	node := qs.ValueOf(name)
	doc := bson.M{
		"_id":  node.(string),
		"Name": toMongoValue(name),
	}
	upsert := bson.M{
		"$setOnInsert": doc,
		"$inc": bson.M{
			"Size": inc,
		},
	}

	_, err := qs.db.C("nodes").UpsertId(node, upsert)
	if err != nil {
		clog.Errorf("Error updating node: %v", err)
	}
	return err
}

func (qs *QuadStore) updateQuad(q quad.Quad, id int64, proc graph.Procedure) error {
	var setname string
	if proc == graph.Add {
		setname = "Added"
	} else if proc == graph.Delete {
		setname = "Deleted"
	}
	upsert := bson.M{
		"$setOnInsert": mongoQuad{
			Subject:   toMongoValue(q.Subject),
			Predicate: toMongoValue(q.Predicate),
			Object:    toMongoValue(q.Object),
			Label:     toMongoValue(q.Label),
		},
		"$push": bson.M{
			setname: id,
		},
	}
	_, err := qs.db.C("quads").UpsertId(qs.getIDForQuad(q), upsert)
	if err != nil {
		clog.Errorf("Error: %v", err)
	}
	return err
}

func (qs *QuadStore) checkValid(key string) bool {
	var indexEntry struct {
		Added   []int64 `bson:"Added"`
		Deleted []int64 `bson:"Deleted"`
	}
	err := qs.db.C("quads").FindId(key).One(&indexEntry)
	if err == mgo.ErrNotFound {
		return false
	}
	if err != nil {
		clog.Errorf("Other error checking valid quad: %s %v.", key, err)
		return false
	}
	if len(indexEntry.Added) <= len(indexEntry.Deleted) {
		return false
	}
	return true
}

func (qs *QuadStore) updateLog(d graph.Delta) error {
	var action string
	if d.Action == graph.Add {
		action = "Add"
	} else {
		action = "Delete"
	}
	entry := MongoLogEntry{
		LogID:     d.ID.Int(),
		Action:    action,
		Key:       qs.getIDForQuad(d.Quad),
		Timestamp: d.Timestamp.UnixNano(),
	}
	err := qs.db.C("log").Insert(entry)
	if err != nil {
		clog.Errorf("Error updating log: %v", err)
	}
	return err
}

func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta, ignoreOpts graph.IgnoreOpts) error {
	qs.session.SetSafe(nil)
	ids := make(map[quad.Value]int)
	// Pre-check the existence condition.
	for _, d := range deltas {
		if d.Action != graph.Add && d.Action != graph.Delete {
			return &graph.DeltaError{Delta: d, Err: graph.ErrInvalidAction}
		}
		key := qs.getIDForQuad(d.Quad)
		switch d.Action {
		case graph.Add:
			if qs.checkValid(key) {
				if ignoreOpts.IgnoreDup {
					continue
				} else {
					return &graph.DeltaError{Delta: d, Err: graph.ErrQuadExists}
				}
			}
		case graph.Delete:
			if !qs.checkValid(key) {
				if ignoreOpts.IgnoreMissing {
					continue
				} else {
					return &graph.DeltaError{Delta: d, Err: graph.ErrQuadNotExist}
				}
			}
		}
	}
	if clog.V(2) {
		clog.Infof("Existence verified. Proceeding.")
	}
	for _, d := range deltas {
		err := qs.updateLog(d)
		if err != nil {
			return &graph.DeltaError{Delta: d, Err: err}
		}
	}
	for _, d := range deltas {
		err := qs.updateQuad(d.Quad, d.ID.Int(), d.Action)
		if err != nil {
			return &graph.DeltaError{Delta: d, Err: err}
		}
		var countdelta int
		if d.Action == graph.Add {
			countdelta = 1
		} else {
			countdelta = -1
		}
		ids[d.Quad.Subject] += countdelta
		ids[d.Quad.Object] += countdelta
		ids[d.Quad.Predicate] += countdelta
		if d.Quad.Label != nil {
			ids[d.Quad.Label] += countdelta
		}
	}
	for k, v := range ids {
		err := qs.updateNodeBy(k, v)
		if err != nil {
			return err
		}
	}
	qs.session.SetSafe(&mgo.Safe{})
	return nil
}

type value interface{}

type mongoQuad struct {
	Subject   value `json:"subject"`
	Predicate value `json:"predicate"`
	Object    value `json:"object"`
	Label     value `json:"label,omitempty"`
}

func toMongoValue(v quad.Value) value {
	if v == nil {
		return nil
	}
	qv := proto.MakeValue(v)
	data, err := qv.Marshal()
	if err != nil {
		panic(err)
	}
	return data
}

func toQuadValue(v value) quad.Value {
	if v == nil {
		return nil
	}
	switch d := v.(type) {
	case []byte:
		var p proto.Value
		if err := p.Unmarshal(d); err != nil {
			clog.Errorf("Error: Couldn't decode value: %v", err)
			return nil
		}
		return p.ToNative()
	default:
		panic(fmt.Errorf("unsupported type: %T", v))
	}
}

func (qs *QuadStore) Quad(val graph.Value) quad.Quad {
	var q mongoQuad
	err := qs.db.C("quads").FindId(val.(string)).One(&q)
	if err != nil {
		clog.Errorf("Error: Couldn't retrieve quad %s %v", val, err)
	}
	return quad.Quad{
		Subject:   toQuadValue(q.Subject),
		Predicate: toQuadValue(q.Predicate),
		Object:    toQuadValue(q.Object),
		Label:     toQuadValue(q.Label),
	}
}

func (qs *QuadStore) QuadIterator(d quad.Direction, val graph.Value) graph.Iterator {
	return NewIterator(qs, "quads", d, val)
}

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator(qs, "nodes")
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return NewAllIterator(qs, "quads")
}

func (qs *QuadStore) ValueOf(s quad.Value) graph.Value {
	return hashOf(s)
}

func (qs *QuadStore) NameOf(v graph.Value) quad.Value {
	val, ok := qs.ids.Get(v.(string))
	if ok {
		return val.(quad.Value)
	}
	var node MongoNode
	err := qs.db.C("nodes").FindId(v.(string)).One(&node)
	if err != nil {
		clog.Errorf("Error: Couldn't retrieve node %s %v", v, err)
	}
	qv := toQuadValue(node.Name)
	if node.ID != "" && qv != nil {
		qs.ids.Put(v.(string), qv)
	}
	return qv
}

func (qs *QuadStore) Size() int64 {
	// TODO(barakmich): Make size real; store it in the log, and retrieve it.
	count, err := qs.db.C("quads").Count()
	if err != nil {
		clog.Errorf("Error: %v", err)
		return 0
	}
	return int64(count)
}

func (qs *QuadStore) Horizon() graph.PrimaryKey {
	var log MongoLogEntry
	err := qs.db.C("log").Find(nil).Sort("-LogID").One(&log)
	if err != nil {
		if err == mgo.ErrNotFound {
			return graph.NewSequentialKey(0)
		}
		clog.Errorf("Could not get Horizon from Mongo: %v", err)
	}
	return graph.NewSequentialKey(log.LogID)
}

func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(iterator.Identity)
}

func (qs *QuadStore) Close() {
	qs.db.Session.Close()
}

func (qs *QuadStore) QuadDirection(in graph.Value, d quad.Direction) graph.Value {
	// Maybe do the trick here
	var offset int
	switch d {
	case quad.Subject:
		offset = 0
	case quad.Predicate:
		offset = (quad.HashSize * 2)
	case quad.Object:
		offset = (quad.HashSize * 2) * 2
	case quad.Label:
		offset = (quad.HashSize * 2) * 3
	}
	val := in.(string)[offset : quad.HashSize*2+offset]
	return val
}

// TODO(barakmich): Rewrite bulk loader. For now, iterating around blocks is the way we'll go about it.

func (qs *QuadStore) Type() string {
	return QuadStoreType
}

func (qs *QuadStore) getSize(collection string, constraint bson.M) (int64, error) {
	var size int
	bytes, err := bson.Marshal(constraint)
	if err != nil {
		clog.Errorf("Couldn't marshal internal constraint")
		return -1, err
	}
	key := collection + string(bytes)
	if val, ok := qs.sizes.Get(key); ok {
		return val.(int64), nil
	}
	if constraint == nil {
		size, err = qs.db.C(collection).Count()
	} else {
		size, err = qs.db.C(collection).Find(constraint).Count()
	}
	if err != nil {
		clog.Errorf("Trouble getting size for iterator! %v", err)
		return -1, err
	}
	qs.sizes.Put(key, int64(size))
	return int64(size), nil
}
