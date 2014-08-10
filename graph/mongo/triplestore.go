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
	"crypto/sha1"
	"encoding/hex"
	"hash"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/barakmich/glog"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

func init() {
	graph.RegisterTripleStore("mongo", true, newTripleStore, createNewMongoGraph)
}

const DefaultDBName = "cayley"

type TripleStore struct {
	session *mgo.Session
	db      *mgo.Database
	hasher  hash.Hash
	idCache *IDLru
}

func createNewMongoGraph(addr string, options graph.Options) error {
	conn, err := mgo.Dial(addr)
	if err != nil {
		return err
	}
	conn.SetSafe(&mgo.Safe{})
	dbName := DefaultDBName
	if val, ok := options.StringKey("database_name"); ok {
		dbName = val
	}
	db := conn.DB(dbName)
	indexOpts := mgo.Index{
		Key:        []string{"Sub"},
		Unique:     false,
		DropDups:   false,
		Background: true,
		Sparse:     true,
	}
	db.C("quads").EnsureIndex(indexOpts)
	indexOpts.Key = []string{"Pred"}
	db.C("quads").EnsureIndex(indexOpts)
	indexOpts.Key = []string{"Obj"}
	db.C("quads").EnsureIndex(indexOpts)
	indexOpts.Key = []string{"Label"}
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

func newTripleStore(addr string, options graph.Options) (graph.TripleStore, error) {
	var qs TripleStore
	conn, err := mgo.Dial(addr)
	if err != nil {
		return nil, err
	}
	conn.SetSafe(&mgo.Safe{})
	dbName := DefaultDBName
	if val, ok := options.StringKey("database_name"); ok {
		dbName = val
	}
	qs.db = conn.DB(dbName)
	qs.session = conn
	qs.hasher = sha1.New()
	qs.idCache = NewIDLru(1 << 16)
	return &qs, nil
}

func (qs *TripleStore) getIdForTriple(t quad.Quad) string {
	id := qs.ConvertStringToByteHash(t.Subject)
	id += qs.ConvertStringToByteHash(t.Predicate)
	id += qs.ConvertStringToByteHash(t.Object)
	id += qs.ConvertStringToByteHash(t.Label)
	return id
}

func (qs *TripleStore) ConvertStringToByteHash(s string) string {
	qs.hasher.Reset()
	key := make([]byte, 0, qs.hasher.Size())
	qs.hasher.Write([]byte(s))
	key = qs.hasher.Sum(key)
	return hex.EncodeToString(key)
}

type MongoNode struct {
	Id   string "_id"
	Name string "Name"
	Size int    "Size"
}

type MongoLogEntry struct {
	LogID     int64  "LogID"
	Action    string "Action"
	Key       string "Key"
	Timestamp int64
}

func (qs *TripleStore) updateNodeBy(node_name string, inc int) error {
	node := qs.ValueOf(node_name)
	doc := bson.M{
		"_id":  node.(string),
		"Name": node_name,
	}
	upsert := bson.M{
		"$setOnInsert": doc,
		"$inc": bson.M{
			"Size": inc,
		},
	}

	_, err := qs.db.C("nodes").UpsertId(node, upsert)
	if err != nil {
		glog.Errorf("Error updating node: %v", err)
	}
	return err
}

func (qs *TripleStore) updateTriple(t quad.Quad, id int64, proc graph.Procedure) error {
	var setname string
	if proc == graph.Add {
		setname = "Added"
	} else if proc == graph.Delete {
		setname = "Deleted"
	}
	tripledoc := bson.M{
		"Subject":   t.Subject,
		"Predicate": t.Predicate,
		"Object":    t.Object,
		"Label":     t.Label,
	}
	upsert := bson.M{
		"$setOnInsert": tripledoc,
		"$push": bson.M{
			setname: id,
		},
	}
	_, err := qs.db.C("quads").UpsertId(qs.getIdForTriple(t), upsert)
	if err != nil {
		glog.Errorf("Error: %v", err)
	}
	return err
}

func (qs *TripleStore) updateLog(d *graph.Delta) error {
	var action string
	if d.Action == graph.Add {
		action = "Add"
	} else {
		action = "Delete"
	}
	entry := MongoLogEntry{
		LogID:     d.ID,
		Action:    action,
		Key:       qs.getIdForTriple(d.Quad),
		Timestamp: d.Timestamp.UnixNano(),
	}
	err := qs.db.C("log").Insert(entry)
	if err != nil {
		glog.Errorf("Error updating log: %v", err)
	}
	return err
}

func (qs *TripleStore) ApplyDeltas(in []*graph.Delta) error {
	qs.session.SetSafe(nil)
	ids := make(map[string]int)
	for _, d := range in {
		err := qs.updateLog(d)
		if err != nil {
			return err
		}
	}
	for _, d := range in {
		err := qs.updateTriple(d.Quad, d.ID, d.Action)
		if err != nil {
			return err
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
		if d.Quad.Label != "" {
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

func (qs *TripleStore) Quad(val graph.Value) quad.Quad {
	var bsonDoc bson.M
	err := qs.db.C("quads").FindId(val.(string)).One(&bsonDoc)
	if err != nil {
		glog.Errorf("Error: Couldn't retrieve quad %s %v", val, err)
	}
	return quad.Quad{
		bsonDoc["Subject"].(string),
		bsonDoc["Predicate"].(string),
		bsonDoc["Object"].(string),
		bsonDoc["Label"].(string),
	}
}

func (qs *TripleStore) TripleIterator(d quad.Direction, val graph.Value) graph.Iterator {
	return NewIterator(qs, "quads", d, val)
}

func (qs *TripleStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator(qs, "nodes")
}

func (qs *TripleStore) TriplesAllIterator() graph.Iterator {
	return NewAllIterator(qs, "quads")
}

func (qs *TripleStore) ValueOf(s string) graph.Value {
	return qs.ConvertStringToByteHash(s)
}

func (qs *TripleStore) NameOf(v graph.Value) string {
	val, ok := qs.idCache.Get(v.(string))
	if ok {
		return val
	}
	var node MongoNode
	err := qs.db.C("nodes").FindId(v.(string)).One(&node)
	if err != nil {
		glog.Errorf("Error: Couldn't retrieve node %s %v", v, err)
	}
	qs.idCache.Put(v.(string), node.Name)
	return node.Name
}

func (qs *TripleStore) Size() int64 {
	// TODO(barakmich): Make size real; store it in the log, and retrieve it.
	count, err := qs.db.C("quads").Count()
	if err != nil {
		glog.Errorf("Error: %v", err)
		return 0
	}
	return int64(count)
}

func (qs *TripleStore) Horizon() int64 {
	var log MongoLogEntry
	err := qs.db.C("log").Find(nil).Sort("-LogID").One(&log)
	if err != nil {
		glog.Errorf("Could not get Horizon from Mongo: %v", err)
	}
	return log.LogID
}

func compareStrings(a, b graph.Value) bool {
	return a.(string) == b.(string)
}

func (qs *TripleStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixedIteratorWithCompare(compareStrings)
}

func (qs *TripleStore) Close() {
	qs.db.Session.Close()
}

func (qs *TripleStore) TripleDirection(in graph.Value, d quad.Direction) graph.Value {
	// Maybe do the trick here
	var offset int
	switch d {
	case quad.Subject:
		offset = 0
	case quad.Predicate:
		offset = (qs.hasher.Size() * 2)
	case quad.Object:
		offset = (qs.hasher.Size() * 2) * 2
	case quad.Label:
		offset = (qs.hasher.Size() * 2) * 3
	}
	val := in.(string)[offset : qs.hasher.Size()*2+offset]
	return val
}

// TODO(barakmich): Rewrite bulk loader. For now, iterating around blocks is the way we'll go about it.
