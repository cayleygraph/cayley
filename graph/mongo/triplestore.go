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
	"log"

	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"

	"github.com/barakmich/glog"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
)

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
	db.C("triples").EnsureIndex(indexOpts)
	indexOpts.Key = []string{"Pred"}
	db.C("triples").EnsureIndex(indexOpts)
	indexOpts.Key = []string{"Obj"}
	db.C("triples").EnsureIndex(indexOpts)
	indexOpts.Key = []string{"Provenance"}
	db.C("triples").EnsureIndex(indexOpts)
	return nil
}

func newTripleStore(addr string, options graph.Options) (graph.TripleStore, error) {
	var ts TripleStore
	conn, err := mgo.Dial(addr)
	if err != nil {
		return nil, err
	}
	conn.SetSafe(&mgo.Safe{})
	dbName := DefaultDBName
	if val, ok := options.StringKey("database_name"); ok {
		dbName = val
	}
	ts.db = conn.DB(dbName)
	ts.session = conn
	ts.hasher = sha1.New()
	ts.idCache = NewIDLru(1 << 16)
	return &ts, nil
}

func (ts *TripleStore) getIdForTriple(t *graph.Triple) string {
	id := ts.ConvertStringToByteHash(t.Subject)
	id += ts.ConvertStringToByteHash(t.Predicate)
	id += ts.ConvertStringToByteHash(t.Object)
	id += ts.ConvertStringToByteHash(t.Provenance)
	return id
}

func (ts *TripleStore) ConvertStringToByteHash(s string) string {
	ts.hasher.Reset()
	key := make([]byte, 0, ts.hasher.Size())
	ts.hasher.Write([]byte(s))
	key = ts.hasher.Sum(key)
	return hex.EncodeToString(key)
}

type MongoNode struct {
	Id   string "_id"
	Name string "Name"
	Size int    "Size"
}

func (ts *TripleStore) updateNodeBy(node_name string, inc int) {
	var size MongoNode
	node := ts.ValueOf(node_name)
	err := ts.db.C("nodes").FindId(node).One(&size)
	if err != nil {
		if err.Error() == "not found" {
			// Not found. Okay.
			size.Id = node.(string)
			size.Name = node_name
			size.Size = inc
		} else {
			glog.Error("Error:", err)
			return
		}
	} else {
		size.Id = node.(string)
		size.Name = node_name
		size.Size += inc
	}

	// Removing something...
	if inc < 0 {
		if size.Size <= 0 {
			err := ts.db.C("nodes").RemoveId(node)
			if err != nil {
				glog.Error("Error: ", err, " while removing node ", node_name)
				return
			}
		}
	}

	_, err2 := ts.db.C("nodes").UpsertId(node, size)
	if err2 != nil {
		glog.Error("Error: ", err)
	}
}

func (ts *TripleStore) writeTriple(t *graph.Triple) bool {
	tripledoc := bson.M{
		"_id":        ts.getIdForTriple(t),
		"Subject":    t.Subject,
		"Predicate":  t.Predicate,
		"Object":     t.Object,
		"Provenance": t.Provenance,
	}
	err := ts.db.C("triples").Insert(tripledoc)
	if err != nil {
		// Among the reasons I hate MongoDB. "Errors don't happen! Right guys?"
		if err.(*mgo.LastError).Code == 11000 {
			return false
		}
		glog.Error("Error: ", err)
		return false
	}
	return true
}

func (ts *TripleStore) AddTriple(t *graph.Triple) {
	_ = ts.writeTriple(t)
	ts.updateNodeBy(t.Subject, 1)
	ts.updateNodeBy(t.Predicate, 1)
	ts.updateNodeBy(t.Object, 1)
	if t.Provenance != "" {
		ts.updateNodeBy(t.Provenance, 1)
	}
}

func (ts *TripleStore) AddTripleSet(in []*graph.Triple) {
	ts.session.SetSafe(nil)
	ids := make(map[string]int)
	for _, t := range in {
		wrote := ts.writeTriple(t)
		if wrote {
			ids[t.Subject]++
			ids[t.Object]++
			ids[t.Predicate]++
			if t.Provenance != "" {
				ids[t.Provenance]++
			}
		}
	}
	for k, v := range ids {
		ts.updateNodeBy(k, v)
	}
	ts.session.SetSafe(&mgo.Safe{})
}

func (ts *TripleStore) RemoveTriple(t *graph.Triple) {
	err := ts.db.C("triples").RemoveId(ts.getIdForTriple(t))
	if err == mgo.ErrNotFound {
		return
	} else if err != nil {
		log.Println("Error: ", err, " while removing triple ", t)
		return
	}
	ts.updateNodeBy(t.Subject, -1)
	ts.updateNodeBy(t.Predicate, -1)
	ts.updateNodeBy(t.Object, -1)
	if t.Provenance != "" {
		ts.updateNodeBy(t.Provenance, -1)
	}
}

func (ts *TripleStore) Triple(val graph.Value) *graph.Triple {
	var bsonDoc bson.M
	err := ts.db.C("triples").FindId(val.(string)).One(&bsonDoc)
	if err != nil {
		log.Println("Error: Couldn't retrieve triple", val.(string), err)
	}
	return &graph.Triple{
		bsonDoc["Subject"].(string),
		bsonDoc["Predicate"].(string),
		bsonDoc["Object"].(string),
		bsonDoc["Provenance"].(string),
	}
}

func (ts *TripleStore) TripleIterator(d graph.Direction, val graph.Value) graph.Iterator {
	return NewIterator(ts, "triples", d, val)
}

func (ts *TripleStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator(ts, "nodes")
}

func (ts *TripleStore) TriplesAllIterator() graph.Iterator {
	return NewAllIterator(ts, "triples")
}

func (ts *TripleStore) ValueOf(s string) graph.Value {
	return ts.ConvertStringToByteHash(s)
}

func (ts *TripleStore) NameOf(v graph.Value) string {
	val, ok := ts.idCache.Get(v.(string))
	if ok {
		return val
	}
	var node MongoNode
	err := ts.db.C("nodes").FindId(v.(string)).One(&node)
	if err != nil {
		log.Println("Error: Couldn't retrieve node", v.(string), err)
	}
	ts.idCache.Put(v.(string), node.Name)
	return node.Name
}

func (ts *TripleStore) Size() int64 {
	count, err := ts.db.C("triples").Count()
	if err != nil {
		glog.Error("Error: ", err)
		return 0
	}
	return int64(count)
}

func compareStrings(a, b graph.Value) bool {
	return a.(string) == b.(string)
}

func (ts *TripleStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixedIteratorWithCompare(compareStrings)
}

func (ts *TripleStore) Close() {
	ts.db.Session.Close()
}

func (ts *TripleStore) TripleDirection(in graph.Value, d graph.Direction) graph.Value {
	// Maybe do the trick here
	var offset int
	switch d {
	case graph.Subject:
		offset = 0
	case graph.Predicate:
		offset = (ts.hasher.Size() * 2)
	case graph.Object:
		offset = (ts.hasher.Size() * 2) * 2
	case graph.Provenance:
		offset = (ts.hasher.Size() * 2) * 3
	}
	val := in.(string)[offset : ts.hasher.Size()*2+offset]
	return val
}

func (ts *TripleStore) BulkLoad(t_chan chan *graph.Triple) bool {
	if ts.Size() != 0 {
		return false
	}

	ts.session.SetSafe(nil)
	for triple := range t_chan {
		ts.writeTriple(triple)
	}
	outputTo := bson.M{"replace": "nodes", "sharded": true}
	glog.Infoln("Mapreducing")
	job := mgo.MapReduce{
		Map: `function() {
      var len = this["_id"].length
      var s_key = this["_id"].slice(0, len / 4)
      var p_key = this["_id"].slice(len / 4, 2 * len / 4)
      var o_key = this["_id"].slice(2 * len / 4, 3 * len / 4)
      var c_key = this["_id"].slice(3 * len / 4)
      emit(s_key, {"_id": s_key, "Name" : this.Subject, "Size" : 1})
      emit(p_key, {"_id": p_key, "Name" : this.Predicate, "Size" : 1})
      emit(o_key, {"_id": o_key, "Name" : this.Object, "Size" : 1})
			if (this.Provenance != "") {
				emit(c_key, {"_id": c_key, "Name" : this.Provenance, "Size" : 1})
			}
    }
    `,
		Reduce: `
      function(key, value_list) {
        out = {"_id": key, "Name": value_list[0].Name}
        count = 0
        for (var i = 0; i < value_list.length; i++) {
          count = count + value_list[i].Size

        }
        out["Size"] = count
        return out
      }
    `,
		Out: outputTo,
	}
	ts.db.C("triples").Find(nil).MapReduce(&job, nil)
	glog.Infoln("Fixing")
	ts.db.Run(bson.D{{"eval", `function() { db.nodes.find().forEach(function (result) {
    db.nodes.update({"_id": result._id}, result.value)
  }) }`}, {"args", bson.D{}}}, nil)

	ts.session.SetSafe(&mgo.Safe{})
	return true
}

func init() {
	graph.RegisterTripleStore("mongo", newTripleStore, createNewMongoGraph)
}
