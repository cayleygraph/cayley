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

package nosql

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/internal/lru"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/quad/pquads"
)

const DefaultDBName = "cayley"

type Registration struct {
	NewFunc      NewFunc
	InitFunc     InitFunc
	IsPersistent bool
}

type InitFunc func(string, graph.Options) (Database, error)
type NewFunc func(string, graph.Options) (Database, error)

func Register(name string, r Registration) {
	graph.RegisterQuadStore(name, graph.QuadStoreRegistration{
		InitFunc: func(addr string, opt graph.Options) error {
			if !r.IsPersistent {
				return nil
			}
			db, err := r.InitFunc(addr, opt)
			if err != nil {
				return err
			}
			defer db.Close()
			if err = Init(db, opt); err != nil {
				return err
			}
			return db.Close()
		},
		NewFunc: func(addr string, opt graph.Options) (graph.QuadStore, error) {
			db, err := r.NewFunc(addr, opt)
			if err != nil {
				return nil, err
			}
			if !r.IsPersistent {
				if err = Init(db, opt); err != nil {
					db.Close()
					return nil, err
				}
			}
			return New(db, opt)
		},
		IsPersistent: r.IsPersistent,
	})
}

func Init(db Database, opt graph.Options) error {
	return ensureIndexes(context.TODO(), db)
}

func New(db Database, opt graph.Options) (graph.QuadStore, error) {
	if err := ensureIndexes(context.TODO(), db); err != nil {
		return nil, err
	}
	qs := &QuadStore{
		db:    db,
		ids:   lru.New(1 << 16),
		sizes: lru.New(1 << 16),
	}
	return qs, nil
}

type NodeHash string

func (NodeHash) IsNode() bool       { return false }
func (v NodeHash) Key() interface{} { return v }

type QuadHash [4]string

func (QuadHash) IsNode() bool       { return false }
func (v QuadHash) Key() interface{} { return v }

func (h QuadHash) Get(d quad.Direction) string {
	var ind int
	switch d {
	case quad.Subject:
		ind = 0
	case quad.Predicate:
		ind = 1
	case quad.Object:
		ind = 2
	case quad.Label:
		ind = 3
	}
	return h[ind]
}

const (
	colLog   = "log"
	colNodes = "nodes"
	colQuads = "quads"

	fldSubject   = "subject"
	fldPredicate = "predicate"
	fldObject    = "object"
	fldLabel     = "label"

	fldID      = "id"
	fldValue   = "Name"
	fldSize    = "Size"
	fldAdded   = "Added"
	fldDeleted = "Deleted"

	fldValData = "val"
	fldIRI     = "iri"
	fldBNode   = "bnode"
	fldType    = "type"
	fldLang    = "lang"
)

type QuadStore struct {
	db    Database
	ids   *lru.Cache
	sizes *lru.Cache
}

func ensureIndexes(ctx context.Context, db Database) error {
	err := db.EnsureIndex(ctx, colLog, Index{
		Fields: []string{fldID},
		Type:   StringExact,
	}, nil)
	if err != nil {
		return err
	}
	err = db.EnsureIndex(ctx, colNodes, Index{
		Fields: []string{fldID},
		Type:   StringExact,
	}, nil)
	if err != nil {
		return err
	}
	err = db.EnsureIndex(ctx, colQuads, Index{
		Fields: []string{
			fldSubject,
			fldPredicate,
			fldObject,
			fldLabel,
		},
		Type: StringExact,
	}, []Index{
		{Fields: []string{fldSubject}, Type: StringExact},
		{Fields: []string{fldPredicate}, Type: StringExact},
		{Fields: []string{fldObject}, Type: StringExact},
		{Fields: []string{fldLabel}, Type: StringExact},
	})
	if err != nil {
		return err
	}
	return nil
}

func (qs *QuadStore) getIDForQuad(t quad.Quad) Key {
	return Key{
		hashOf(t.Subject),
		hashOf(t.Predicate),
		hashOf(t.Object),
		hashOf(t.Label),
	}
}

func hashOf(s quad.Value) string {
	if s == nil {
		return ""
	}
	return hex.EncodeToString(quad.HashOf(s))
}

type node struct {
	ID   string   `json:"id"`
	Name Document `json:"Name"`
	Size int      `json:"Size"`
}

type logEntry struct {
	ID     string `json:"id"`
	Action string `json:"Action"`
	Key    string `json:"Key"`
}

func (qs *QuadStore) updateNodeBy(ctx context.Context, name quad.Value, inc int) error {
	_ = node{}
	node := qs.ValueOf(name)
	key := Key{string(node.(NodeHash))}

	err := qs.db.Update(colNodes, key).Upsert(Document{
		fldValue: toDocumentValue(name),
	}).Inc(fldSize, inc).Do(ctx)
	if err != nil {
		clog.Errorf("Error updating node: %v", err)
	}
	if inc < 0 {
		err = qs.db.Delete(colNodes).Keys(key).WithFields(FieldFilter{
			Path:   []string{fldSize},
			Filter: Equal,
			Value:  Int(0),
		}).Do(ctx)
		if err != nil {
			clog.Errorf("Error deleting empty node: %v", err)
		}
	}
	return err
}

func (qs *QuadStore) updateQuad(ctx context.Context, q quad.Quad, key Key, proc graph.Procedure) error {
	var setname string
	if proc == graph.Add {
		setname = fldAdded
	} else if proc == graph.Delete {
		setname = fldDeleted
	}
	doc := Document{
		fldSubject:   String(hashOf(q.Subject)),
		fldPredicate: String(hashOf(q.Predicate)),
		fldObject:    String(hashOf(q.Object)),
	}
	if l := hashOf(q.Label); l != "" {
		doc[fldLabel] = String(l)
	}
	if len(key) != 1 {
		// we should not push vector keys to arrays
		panic(fmt.Errorf("unexpected key: %v", key))
	}
	err := qs.db.Update(colQuads, qs.getIDForQuad(q)).Upsert(doc).
		Inc(setname, 1).Do(ctx)
	if err != nil {
		clog.Errorf("Error: %v", err)
	}
	return err
}

func checkQuadValid(q Document) bool {
	added, _ := q[fldAdded].(Int)
	deleted, _ := q[fldDeleted].(Int)
	return added > deleted
}

func (qs *QuadStore) checkValidQuad(ctx context.Context, key Key) bool {
	q, err := qs.db.FindByKey(ctx, colQuads, key)
	if err == ErrNotFound {
		return false
	}
	if err != nil {
		clog.Errorf("Other error checking valid quad: %s %v.", key, err)
		return false
	}
	return checkQuadValid(q)
}

func (qs *QuadStore) updateLog(ctx context.Context, d *graph.Delta) (Key, error) {
	var action string
	if d.Action == graph.Add {
		action = "Add"
	} else {
		action = "Delete"
	}
	_ = logEntry{}
	return qs.db.Insert(ctx, colLog, nil, Document{
		"Action": String(action),
		"Key":    qs.getIDForQuad(d.Quad).Value(),
	})
}

func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta, ignoreOpts graph.IgnoreOpts) error {
	ctx := context.TODO()
	ids := make(map[quad.Value]int)
	// Pre-check the existence condition.
	for _, d := range deltas {
		if d.Action != graph.Add && d.Action != graph.Delete {
			return &graph.DeltaError{Delta: d, Err: graph.ErrInvalidAction}
		}
		key := qs.getIDForQuad(d.Quad)
		switch d.Action {
		case graph.Add:
			if qs.checkValidQuad(ctx, key) {
				if ignoreOpts.IgnoreDup {
					continue
				} else {
					return &graph.DeltaError{Delta: d, Err: graph.ErrQuadExists}
				}
			}
		case graph.Delete:
			if !qs.checkValidQuad(ctx, key) {
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
	oids := make([]Key, 0, len(deltas))
	for i, d := range deltas {
		id, err := qs.updateLog(ctx, &deltas[i])
		if err != nil {
			return &graph.DeltaError{Delta: d, Err: err}
		}
		oids = append(oids, id)
	}
	// make sure to create all nodes before writing any quads
	// concurrent reads may observe broken quads in other case
	for k, v := range ids {
		err := qs.updateNodeBy(ctx, k, v)
		if err != nil {
			return err
		}
	}
	for i, d := range deltas {
		err := qs.updateQuad(ctx, d.Quad, oids[i], d.Action)
		if err != nil {
			return &graph.DeltaError{Delta: d, Err: err}
		}
	}
	return nil
}

type nodeValue struct {
	Value   string `json:"val"`
	IsIRI   bool   `json:"iri,omitempty"`
	IsBNode bool   `json:"bnode,omitempty"`
	Type    string `json:"type,omitempty"`
	Lang    string `json:"lang,omitempty"`
}

func toDocumentValue(v quad.Value) Value {
	if v == nil {
		return nil
	}
	_ = nodeValue{}
	switch d := v.(type) {
	case quad.Raw:
		return String(d) // compatibility
	case quad.String:
		return Document{fldValData: String(d)}
	case quad.IRI:
		return Document{fldValData: String(d), fldIRI: Bool(true)}
	case quad.BNode:
		return Document{fldValData: String(d), fldBNode: Bool(true)}
	case quad.TypedString:
		return Document{fldValData: String(d.Value), fldType: String(d.Type)}
	case quad.LangString:
		return Document{fldValData: String(d.Value), fldLang: String(d.Lang)}
	case quad.Int:
		return Int(d)
	case quad.Float:
		return Float(d)
	case quad.Bool:
		return Bool(d)
	case quad.Time:
		return Time(d)
	default:
		qv := pquads.MakeValue(v)
		data, err := qv.Marshal()
		if err != nil {
			panic(err)
		}
		return Bytes(data)
	}
}

func toQuadValue(v Value) quad.Value {
	if v == nil {
		return nil
	}
	_ = nodeValue{}
	switch d := v.(type) {
	case String:
		return quad.Raw(d) // compatibility
	case Int:
		return quad.Int(d)
	case Float:
		return quad.Float(d)
	case Bool:
		return quad.Bool(d)
	case Time:
		return quad.Time(d)
	case Document: // TODO(dennwc): use raw document instead?
		s, ok := d[fldValData].(String)
		if !ok {
			clog.Errorf("Error: Empty value in map: %v", v)
			return nil
		}
		if len(d) == 1 {
			return quad.String(s)
		}
		if o, ok := d[fldIRI].(Bool); ok && bool(o) {
			return quad.IRI(s)
		} else if o, ok := d[fldBNode].(Bool); ok && bool(o) {
			return quad.BNode(s)
		} else if o, ok := d[fldLang].(String); ok && o != "" {
			return quad.LangString{
				Value: quad.String(s),
				Lang:  string(o),
			}
		} else if o, ok := d[fldType].(String); ok && o != "" {
			return quad.TypedString{
				Value: quad.String(s),
				Type:  quad.IRI(string(o)),
			}
		}
		return quad.String(s)
	case Bytes:
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
	_ = node{}
	nd, err := qs.db.FindByKey(context.TODO(), colNodes, Key{string(hash)})
	if err != nil {
		clog.Errorf("Error: Couldn't retrieve node %s %v", v, err)
	}
	id, _ := nd[fldID].(String)
	dv, _ := nd[fldValue]
	qv := toQuadValue(dv)
	if id != "" && qv != nil {
		qs.ids.Put(string(hash), qv)
	}
	return qv
}

func (qs *QuadStore) Size() int64 {
	// TODO(barakmich): Make size real; store it in the log, and retrieve it.
	count, err := qs.db.Query(colQuads).Count(context.TODO())
	if err != nil {
		clog.Errorf("%v", err)
		return 0
	}
	return count
}

func (qs *QuadStore) Horizon() graph.PrimaryKey {
	// FIXME: this picks a random record; we need to sort at least on timestamp,
	// or emulate a global counter and use it as an id for log entries
	log, err := qs.db.Query(colLog).One(context.TODO())
	if err != nil {
		if err == ErrNotFound {
			return graph.NewSequentialKey(0)
		}
		clog.Errorf("could not get horizon: %v", err)
	}
	_ = logEntry{}
	var id string
	if v, ok := log[fldID].(String); ok {
		id = string(v)
	}
	if id == "" {
		return graph.PrimaryKey{}
	}
	return graph.NewUniqueKey(id)
}

func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(iterator.Identity)
}

func (qs *QuadStore) Close() error {
	return qs.db.Close()
}

func (qs *QuadStore) QuadDirection(in graph.Value, d quad.Direction) graph.Value {
	return NodeHash(in.(QuadHash).Get(d))
}

// TODO(barakmich): Rewrite bulk loader. For now, iterating around blocks is the way we'll go about it.

func (qs *QuadStore) getSize(col string, constraints []FieldFilter) (int64, error) {
	cacheKey := ""
	for _, c := range constraints { // FIXME
		cacheKey += fmt.Sprint(c.Path, c.Filter, c.Value)
	}
	key := col + cacheKey
	if val, ok := qs.sizes.Get(key); ok {
		return val.(int64), nil
	}
	q := qs.db.Query(col)
	if len(constraints) != 0 {
		q = q.WithFields(constraints...)
	}
	size, err := q.Count(context.TODO())
	if err != nil {
		clog.Errorf("error getting size for iterator: %v", err)
		return -1, err
	}
	qs.sizes.Put(key, int64(size))
	return int64(size), nil
}
