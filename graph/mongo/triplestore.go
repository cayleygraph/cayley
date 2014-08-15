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
	"io"
	"sync"

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

// Guarantee we satisfy graph.Bulkloader.
var _ graph.BulkLoader = (*TripleStore)(nil)

const DefaultDBName = "cayley"

var (
	hashPool = sync.Pool{
		New: func() interface{} { return sha1.New() },
	}
	hashSize = sha1.Size
)

type TripleStore struct {
	session *mgo.Session
	db      *mgo.Database
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
	indexOpts.Key = []string{"Label"}
	db.C("triples").EnsureIndex(indexOpts)
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
	qs.idCache = NewIDLru(1 << 16)
	return &qs, nil
}

func (qs *TripleStore) getIdForTriple(t quad.Quad) string {
	id := qs.convertStringToByteHash(t.Subject)
	id += qs.convertStringToByteHash(t.Predicate)
	id += qs.convertStringToByteHash(t.Object)
	id += qs.convertStringToByteHash(t.Label)
	return id
}

func (qs *TripleStore) convertStringToByteHash(s string) string {
	h := hashPool.Get().(hash.Hash)
	h.Reset()
	defer hashPool.Put(h)

	key := make([]byte, 0, hashSize)
	h.Write([]byte(s))
	key = h.Sum(key)
	return hex.EncodeToString(key)
}

type MongoNode struct {
	Id   string "_id"
	Name string "Name"
	Size int    "Size"
}

func (qs *TripleStore) updateNodeBy(node_name string, inc int) {
	var size MongoNode
	node := qs.ValueOf(node_name)
	err := qs.db.C("nodes").FindId(node).One(&size)
	if err != nil {
		if err.Error() == "not found" {
			// Not found. Okay.
			size.Id = node.(string)
			size.Name = node_name
			size.Size = inc
		} else {
			glog.Errorf("Error: %v", err)
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
			err := qs.db.C("nodes").RemoveId(node)
			if err != nil {
				glog.Errorf("Error: %v while removing node %s", err, node_name)
				return
			}
		}
	}

	_, err2 := qs.db.C("nodes").UpsertId(node, size)
	if err2 != nil {
		glog.Errorf("Error: %v", err)
	}
}

func (qs *TripleStore) writeTriple(t quad.Quad) bool {
	tripledoc := bson.M{
		"_id":       qs.getIdForTriple(t),
		"Subject":   t.Subject,
		"Predicate": t.Predicate,
		"Object":    t.Object,
		"Label":     t.Label,
	}
	err := qs.db.C("triples").Insert(tripledoc)
	if err != nil {
		// Among the reasons I hate MongoDB. "Errors don't happen! Right guys?"
		if err.(*mgo.LastError).Code == 11000 {
			return false
		}
		glog.Errorf("Error: %v", err)
		return false
	}
	return true
}

func (qs *TripleStore) AddTriple(t quad.Quad) {
	_ = qs.writeTriple(t)
	qs.updateNodeBy(t.Subject, 1)
	qs.updateNodeBy(t.Predicate, 1)
	qs.updateNodeBy(t.Object, 1)
	if t.Label != "" {
		qs.updateNodeBy(t.Label, 1)
	}
}

func (qs *TripleStore) AddTripleSet(in []quad.Quad) {
	qs.session.SetSafe(nil)
	ids := make(map[string]int)
	for _, t := range in {
		wrote := qs.writeTriple(t)
		if wrote {
			ids[t.Subject]++
			ids[t.Object]++
			ids[t.Predicate]++
			if t.Label != "" {
				ids[t.Label]++
			}
		}
	}
	for k, v := range ids {
		qs.updateNodeBy(k, v)
	}
	qs.session.SetSafe(&mgo.Safe{})
}

func (qs *TripleStore) RemoveTriple(t quad.Quad) {
	err := qs.db.C("triples").RemoveId(qs.getIdForTriple(t))
	if err == mgo.ErrNotFound {
		return
	} else if err != nil {
		glog.Errorf("Error: %v while removing triple %v", err, t)
		return
	}
	qs.updateNodeBy(t.Subject, -1)
	qs.updateNodeBy(t.Predicate, -1)
	qs.updateNodeBy(t.Object, -1)
	if t.Label != "" {
		qs.updateNodeBy(t.Label, -1)
	}
}

func (qs *TripleStore) Quad(val graph.Value) quad.Quad {
	var bsonDoc bson.M
	err := qs.db.C("triples").FindId(val.(string)).One(&bsonDoc)
	if err != nil {
		glog.Errorf("Error: Couldn't retrieve triple %s %v", val, err)
	}
	return quad.Quad{
		bsonDoc["Subject"].(string),
		bsonDoc["Predicate"].(string),
		bsonDoc["Object"].(string),
		bsonDoc["Label"].(string),
	}
}

func (qs *TripleStore) TripleIterator(d quad.Direction, val graph.Value) graph.Iterator {
	return NewIterator(qs, "triples", d, val)
}

func (qs *TripleStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator(qs, "nodes")
}

func (qs *TripleStore) TriplesAllIterator() graph.Iterator {
	return NewAllIterator(qs, "triples")
}

func (qs *TripleStore) ValueOf(s string) graph.Value {
	return qs.convertStringToByteHash(s)
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
	count, err := qs.db.C("triples").Count()
	if err != nil {
		glog.Errorf("Error: %v", err)
		return 0
	}
	return int64(count)
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
		offset = (hashSize * 2)
	case quad.Object:
		offset = (hashSize * 2) * 2
	case quad.Label:
		offset = (hashSize * 2) * 3
	}
	val := in.(string)[offset : hashSize*2+offset]
	return val
}

func (qs *TripleStore) BulkLoad(dec quad.Unmarshaler) error {
	if qs.Size() != 0 {
		return graph.ErrCannotBulkLoad
	}

	qs.session.SetSafe(nil)
	for {
		q, err := dec.Unmarshal()
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
		qs.writeTriple(q)
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
			if (this.Label != "") {
				emit(c_key, {"_id": c_key, "Name" : this.Label, "Size" : 1})
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
	qs.db.C("triples").Find(nil).MapReduce(&job, nil)
	glog.Infoln("Fixing")
	qs.db.Run(bson.D{{"eval", `function() { db.nodes.find().forEach(function (result) {
    db.nodes.update({"_id": result._id}, result.value)
  }) }`}, {"args", bson.D{}}}, nil)

	qs.session.SetSafe(&mgo.Safe{})

	return nil
}
