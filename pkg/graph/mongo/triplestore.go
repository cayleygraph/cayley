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
	"github.com/barakmich/glog"
	"github.com/google/cayley/pkg/graph"
	"hash"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
)

const DefaultDBName = "cayley"

type MongoTripleStore struct {
	session *mgo.Session
	db      *mgo.Database
	hasher  hash.Hash
	idCache *IDLru
}

func CreateNewMongoGraph(addr string, options graph.OptionsDict) bool {
	conn, err := mgo.Dial(addr)
	if err != nil {
		glog.Fatal("Error connecting: ", err)
		return false
	}
	conn.SetSafe(&mgo.Safe{})
	dbName := DefaultDBName
	if val, ok := options.GetStringKey("database_name"); ok {
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
	return true
}

func NewMongoTripleStore(addr string, options graph.OptionsDict) *MongoTripleStore {
	var ts MongoTripleStore
	conn, err := mgo.Dial(addr)
	if err != nil {
		glog.Fatal("Error connecting: ", err)
	}
	conn.SetSafe(&mgo.Safe{})
	dbName := DefaultDBName
	if val, ok := options.GetStringKey("database_name"); ok {
		dbName = val
	}
	ts.db = conn.DB(dbName)
	ts.session = conn
	ts.hasher = sha1.New()
	ts.idCache = NewIDLru(1 << 16)
	return &ts
}

func (ts *MongoTripleStore) getIdForTriple(t *graph.Triple) string {
	id := ts.ConvertStringToByteHash(t.Sub)
	id += ts.ConvertStringToByteHash(t.Pred)
	id += ts.ConvertStringToByteHash(t.Obj)
	id += ts.ConvertStringToByteHash(t.Provenance)
	return id
}

func (ts *MongoTripleStore) ConvertStringToByteHash(s string) string {
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

func (ts *MongoTripleStore) updateNodeBy(node_name string, inc int) {
	var size MongoNode
	node := ts.GetIdFor(node_name)
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

func (ts *MongoTripleStore) writeTriple(t *graph.Triple) bool {
	tripledoc := bson.M{"_id": ts.getIdForTriple(t), "Sub": t.Sub, "Pred": t.Pred, "Obj": t.Obj, "Provenance": t.Provenance}
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

func (ts *MongoTripleStore) AddTriple(t *graph.Triple) {
	_ = ts.writeTriple(t)
	ts.updateNodeBy(t.Sub, 1)
	ts.updateNodeBy(t.Pred, 1)
	ts.updateNodeBy(t.Obj, 1)
	if t.Provenance != "" {
		ts.updateNodeBy(t.Provenance, 1)
	}
}

func (ts *MongoTripleStore) AddTripleSet(in []*graph.Triple) {
	ts.session.SetSafe(nil)
	idMap := make(map[string]int)
	for _, t := range in {
		wrote := ts.writeTriple(t)
		if wrote {
			idMap[t.Sub]++
			idMap[t.Obj]++
			idMap[t.Pred]++
			if t.Provenance != "" {
				idMap[t.Provenance]++
			}
		}
	}
	for k, v := range idMap {
		ts.updateNodeBy(k, v)
	}
	ts.session.SetSafe(&mgo.Safe{})
}

func (ts *MongoTripleStore) RemoveTriple(t *graph.Triple) {
	err := ts.db.C("triples").RemoveId(ts.getIdForTriple(t))
	if err == mgo.ErrNotFound {
		return
	} else if err != nil {
		log.Println("Error: ", err, " while removing triple ", t)
		return
	}
	ts.updateNodeBy(t.Sub, -1)
	ts.updateNodeBy(t.Pred, -1)
	ts.updateNodeBy(t.Obj, -1)
	if t.Provenance != "" {
		ts.updateNodeBy(t.Provenance, -1)
	}
}

func (ts *MongoTripleStore) GetTriple(val graph.TSVal) *graph.Triple {
	var bsonDoc bson.M
	err := ts.db.C("triples").FindId(val.(string)).One(&bsonDoc)
	if err != nil {
		log.Println("Error: Couldn't retrieve triple", val.(string), err)
	}
	return graph.MakeTriple(
		bsonDoc["Sub"].(string),
		bsonDoc["Pred"].(string),
		bsonDoc["Obj"].(string),
		bsonDoc["Provenance"].(string))
}

func (ts *MongoTripleStore) GetTripleIterator(dir string, val graph.TSVal) graph.Iterator {
	return NewMongoIterator(ts, "triples", dir, val)
}

func (ts *MongoTripleStore) GetNodesAllIterator() graph.Iterator {
	return NewMongoAllIterator(ts, "nodes")
}

func (ts *MongoTripleStore) GetTriplesAllIterator() graph.Iterator {
	return NewMongoAllIterator(ts, "triples")
}

func (ts *MongoTripleStore) GetIdFor(s string) graph.TSVal {
	return ts.ConvertStringToByteHash(s)
}

func (ts *MongoTripleStore) GetNameFor(v graph.TSVal) string {
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

func (ts *MongoTripleStore) Size() int64 {
	count, err := ts.db.C("triples").Count()
	if err != nil {
		glog.Error("Error: ", err)
		return 0
	}
	return int64(count)
}

func compareStrings(a, b graph.TSVal) bool {
	return a.(string) == b.(string)
}

func (ts *MongoTripleStore) MakeFixed() *graph.FixedIterator {
	return graph.NewFixedIteratorWithCompare(compareStrings)
}

func (ts *MongoTripleStore) Close() {
	ts.db.Session.Close()
}

func (ts *MongoTripleStore) GetTripleDirection(in graph.TSVal, dir string) graph.TSVal {
	// Maybe do the trick here
	var offset int
	switch dir {
	case "s":
		offset = 0
	case "p":
		offset = (ts.hasher.Size() * 2)
	case "o":
		offset = (ts.hasher.Size() * 2) * 2
	case "c":
		offset = (ts.hasher.Size() * 2) * 3
	}
	val := in.(string)[offset : ts.hasher.Size()*2+offset]
	return val
}

func (ts *MongoTripleStore) BulkLoad(t_chan chan *graph.Triple) {
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
      emit(s_key, {"_id": s_key, "Name" : this.Sub, "Size" : 1})
      emit(p_key, {"_id": p_key, "Name" : this.Pred, "Size" : 1})
      emit(o_key, {"_id": o_key, "Name" : this.Obj, "Size" : 1})
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
}
