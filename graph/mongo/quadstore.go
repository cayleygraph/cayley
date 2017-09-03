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
	"strings"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/internal/lru"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/quad/pquads"
)

const DefaultDBName = "cayley"
const QuadStoreType = "mongo"

func init() {
	graph.RegisterQuadStore(QuadStoreType, graph.QuadStoreRegistration{
		NewFunc:      newQuadStore,
		UpgradeFunc:  nil,
		InitFunc:     createNewMongoGraph,
		IsPersistent: true,
	})
}

type NodeHash string

func (NodeHash) IsNode() bool       { return false }
func (v NodeHash) Key() interface{} { return v }

type QuadHash string

func (QuadHash) IsNode() bool       { return false }
func (v QuadHash) Key() interface{} { return v }

func (h QuadHash) Get(d quad.Direction) string {
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
		if len(h) == offset { // no label
			return ""
		}
	}
	return string(h[offset : quad.HashSize*2+offset])
}

type QuadStore struct {
	session *mgo.Session
	db      *mgo.Database
	ids     *lru.Cache
	sizes   *lru.Cache
}

func ensureIndexes(db *mgo.Database) error {
	indexOpts := mgo.Index{
		Key:        []string{"subject"},
		Unique:     false,
		DropDups:   false,
		Background: true,
		Sparse:     true,
	}
	if err := db.C("quads").EnsureIndex(indexOpts); err != nil {
		return err
	}
	indexOpts.Key = []string{"predicate"}
	if err := db.C("quads").EnsureIndex(indexOpts); err != nil {
		return err
	}
	indexOpts.Key = []string{"object"}
	if err := db.C("quads").EnsureIndex(indexOpts); err != nil {
		return err
	}
	indexOpts.Key = []string{"label"}
	if err := db.C("quads").EnsureIndex(indexOpts); err != nil {
		return err
	}
	return nil
}

func createNewMongoGraph(addr string, options graph.Options) error {
	conn, err := dialMongo(addr, options)
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetSafe(&mgo.Safe{})
	db := conn.DB("")
	return ensureIndexes(db)
}

func dialMongo(addr string, options graph.Options) (*mgo.Session, error) {
	if connVal, ok := options["session"]; ok {
		if conn, ok := connVal.(*mgo.Session); ok {
			return conn, nil
		}
	}
	if strings.HasPrefix(addr, "mongodb://") || strings.ContainsAny(addr, `@/\`) {
		// full mongodb url
		return mgo.Dial(addr)
	}
	var dialInfo mgo.DialInfo
	dialInfo.Addrs = strings.Split(addr, ",")
	user, ok, err := options.StringKey("username")
	if err != nil {
		return nil, err
	}
	if ok {
		dialInfo.Username = user
		password, ok, err := options.StringKey("password")
		if err != nil {
			return nil, err
		}
		if ok {
			dialInfo.Password = password
		}
	}
	dbName := DefaultDBName
	val, ok, err := options.StringKey("database_name")
	if err != nil {
		return nil, err
	}
	if ok {
		dbName = val
	}
	dialInfo.Database = dbName
	return mgo.DialWithInfo(&dialInfo)
}

func newQuadStore(addr string, options graph.Options) (graph.QuadStore, error) {
	var qs QuadStore
	conn, err := dialMongo(addr, options)
	if err != nil {
		return nil, err
	}
	conn.SetSafe(&mgo.Safe{})
	qs.db = conn.DB("")
	if err := ensureIndexes(qs.db); err != nil {
		conn.Close()
		return nil, err
	}
	qs.session = conn
	qs.ids = lru.New(1 << 16)
	qs.sizes = lru.New(1 << 16)
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
	if s == nil {
		return ""
	}
	return hex.EncodeToString(quad.HashOf(s))
}

type MongoNode struct {
	ID   string `bson:"_id"`
	Name value  `bson:"Name"`
	Size int    `bson:"Size"`
}

type MongoLogEntry struct {
	ID     bson.ObjectId `bson:"_id"`
	Action string        `bson:"Action"`
	Key    string        `bson:"Key"`
}

func (qs *QuadStore) updateNodeBy(name quad.Value, inc int) error {
	node := qs.ValueOf(name)
	doc := bson.M{
		"_id":  string(node.(NodeHash)),
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
	if inc < 0 {
		err = qs.db.C("nodes").Remove(bson.M{
			"_id":  string(node.(NodeHash)),
			"Size": 0,
		})
		if err != nil {
			clog.Errorf("Error deleting empty node: %v", err)
		}
	}
	return err
}

func (qs *QuadStore) updateQuad(q quad.Quad, id bson.ObjectId, proc graph.Procedure) error {
	var setname string
	if proc == graph.Add {
		setname = "Added"
	} else if proc == graph.Delete {
		setname = "Deleted"
	}
	upsert := bson.M{
		"$setOnInsert": mongoQuad{
			Subject:   hashOf(q.Subject),
			Predicate: hashOf(q.Predicate),
			Object:    hashOf(q.Object),
			Label:     hashOf(q.Label),
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
		Added   []bson.Raw `bson:"Added"`
		Deleted []bson.Raw `bson:"Deleted"`
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

func objidString(id bson.ObjectId) string {
	return hex.EncodeToString([]byte(id))
}

func (qs *QuadStore) updateLog(d *graph.Delta) (bson.ObjectId, error) {
	var action string
	if d.Action == graph.Add {
		action = "Add"
	} else {
		action = "Delete"
	}
	entry := MongoLogEntry{
		ID:     bson.NewObjectId(),
		Action: action,
		Key:    qs.getIDForQuad(d.Quad),
	}
	err := qs.db.C("log").Insert(entry)
	if err != nil {
		clog.Errorf("Error updating log: %v", err)
		return "", err
	}
	return entry.ID, nil
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
		var dn int
		if d.Action == graph.Add {
			dn = 1
		} else {
			dn = -1
		}
		ids[d.Quad.Subject] += dn
		ids[d.Quad.Object] += dn
		ids[d.Quad.Predicate] += dn
		if d.Quad.Label != nil {
			ids[d.Quad.Label] += dn
		}
	}
	if clog.V(2) {
		clog.Infof("Existence verified. Proceeding.")
	}
	oids := make([]bson.ObjectId, 0, len(deltas))
	for i, d := range deltas {
		id, err := qs.updateLog(&deltas[i])
		if err != nil {
			return &graph.DeltaError{Delta: d, Err: err}
		}
		oids = append(oids, id)
	}
	// make sure to create all nodes before writing any quads
	// concurrent reads may observe broken quads in other case
	for k, v := range ids {
		err := qs.updateNodeBy(k, v)
		if err != nil {
			return err
		}
	}
	for i, d := range deltas {
		err := qs.updateQuad(d.Quad, oids[i], d.Action)
		if err != nil {
			return &graph.DeltaError{Delta: d, Err: err}
		}
	}
	qs.session.SetSafe(&mgo.Safe{})
	return nil
}

type value interface{}

type mongoQuad struct {
	Subject   string `json:"subject"`
	Predicate string `json:"predicate"`
	Object    string `json:"object"`
	Label     string `json:"label,omitempty"`
}

type mongoString struct {
	Value   string `bson:"val"`
	IsIRI   bool   `bson:"iri,omitempty"`
	IsBNode bool   `bson:"bnode,omitempty"`
	Type    string `bson:"type,omitempty"`
	Lang    string `bson:"lang,omitempty"`
}

func toMongoValue(v quad.Value) value {
	if v == nil {
		return nil
	}
	switch d := v.(type) {
	case quad.Raw:
		return string(d) // compatibility
	case quad.String:
		return mongoString{Value: string(d)}
	case quad.IRI:
		return mongoString{Value: string(d), IsIRI: true}
	case quad.BNode:
		return mongoString{Value: string(d), IsBNode: true}
	case quad.TypedString:
		return mongoString{Value: string(d.Value), Type: string(d.Type)}
	case quad.LangString:
		return mongoString{Value: string(d.Value), Lang: string(d.Lang)}
	case quad.Int:
		return int64(d)
	case quad.Float:
		return float64(d)
	case quad.Bool:
		return bool(d)
	case quad.Time:
		// TODO(dennwc): mongo supports only ms precision
		// we can alternatively switch to protobuf serialization instead
		// (maybe add an option for this)
		return time.Time(d)
	default:
		qv := pquads.MakeValue(v)
		data, err := qv.Marshal()
		if err != nil {
			panic(err)
		}
		return data
	}
}

func toQuadValue(v value) quad.Value {
	if v == nil {
		return nil
	}
	switch d := v.(type) {
	case string:
		return quad.Raw(d) // compatibility
	case int64:
		return quad.Int(d)
	case float64:
		return quad.Float(d)
	case bool:
		return quad.Bool(d)
	case time.Time:
		return quad.Time(d)
	case bson.M: // TODO(dennwc): use raw document instead?
		so, ok := d["val"]
		if !ok {
			clog.Errorf("Error: Empty value in map: %v", v)
			return nil
		}
		s := so.(string)
		if len(d) == 1 {
			return quad.String(s)
		}
		if o, ok := d["iri"]; ok && o.(bool) {
			return quad.IRI(s)
		} else if o, ok := d["bnode"]; ok && o.(bool) {
			return quad.BNode(s)
		} else if o, ok := d["lang"]; ok && o.(string) != "" {
			return quad.LangString{
				Value: quad.String(s),
				Lang:  o.(string),
			}
		} else if o, ok := d["type"]; ok && o.(string) != "" {
			return quad.TypedString{
				Value: quad.String(s),
				Type:  quad.IRI(o.(string)),
			}
		}
		return quad.String(s)
	case []byte:
		var p pquads.Value
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
	h := val.(QuadHash)
	return quad.Quad{
		Subject:   qs.NameOf(NodeHash(h.Get(quad.Subject))),
		Predicate: qs.NameOf(NodeHash(h.Get(quad.Predicate))),
		Object:    qs.NameOf(NodeHash(h.Get(quad.Object))),
		Label:     qs.NameOf(NodeHash(h.Get(quad.Label))),
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
	return NodeHash(hashOf(s))
}

func (qs *QuadStore) NameOf(v graph.Value) quad.Value {
	if v == nil {
		return nil
	} else if v, ok := v.(graph.PreFetchedValue); ok {
		return v.NameOf()
	}
	hash := v.(NodeHash)
	if hash == "" {
		return nil
	}
	if val, ok := qs.ids.Get(string(hash)); ok {
		return val.(quad.Value)
	}
	var node MongoNode
	err := qs.db.C("nodes").FindId(string(hash)).One(&node)
	if err != nil {
		clog.Errorf("Error: Couldn't retrieve node %s %v", v, err)
	}
	qv := toQuadValue(node.Name)
	if node.ID != "" && qv != nil {
		qs.ids.Put(string(hash), qv)
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
	err := qs.db.C("log").Find(nil).Sort("-_id").One(&log)
	if err != nil {
		if err == mgo.ErrNotFound {
			return graph.NewSequentialKey(0)
		}
		clog.Errorf("Could not get Horizon from Mongo: %v", err)
	}
	return graph.NewUniqueKey(objidString(log.ID))
}

func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(iterator.Identity)
}

func (qs *QuadStore) Close() error {
	qs.db.Session.Close()
	return nil
}

func (qs *QuadStore) QuadDirection(in graph.Value, d quad.Direction) graph.Value {
	return NodeHash(in.(QuadHash).Get(d))
}

// TODO(barakmich): Rewrite bulk loader. For now, iterating around blocks is the way we'll go about it.

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
