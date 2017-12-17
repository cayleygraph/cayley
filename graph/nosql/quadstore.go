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
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"time"

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
func (v NodeHash) key() Key         { return Key{string(v)} }

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

	fldLogID = "id"

	fldSubject     = "subject"
	fldPredicate   = "predicate"
	fldObject      = "object"
	fldLabel       = "label"
	fldQuadAdded   = "added"
	fldQuadDeleted = "deleted"

	fldHash  = "hash"
	fldValue = "value"
	fldSize  = "refs"

	fldValData  = "str"
	fldIRI      = "iri"
	fldBNode    = "bnode"
	fldType     = "type"
	fldLang     = "lang"
	fldRaw      = "raw"
	fldValInt   = "int"
	fldValFloat = "float"
	fldValBool  = "bool"
	fldValTime  = "ts"
	fldValPb    = "pb"
)

type QuadStore struct {
	db    Database
	ids   *lru.Cache
	sizes *lru.Cache
}

func ensureIndexes(ctx context.Context, db Database) error {
	err := db.EnsureIndex(ctx, colLog, Index{
		Fields: []string{fldLogID},
		Type:   StringExact,
	}, nil)
	if err != nil {
		return err
	}
	err = db.EnsureIndex(ctx, colNodes, Index{
		Fields: []string{fldHash},
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

func getKeyForQuad(t quad.Quad) Key {
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

func (qs *QuadStore) nameToKey(name quad.Value) Key {
	node := qs.hashOf(name)
	return node.key()
}

func (qs *QuadStore) updateNodeBy(ctx context.Context, key Key, name quad.Value, inc int) error {
	if inc == 0 {
		return nil
	}
	d := toDocumentValue(name)
	err := qs.db.Update(colNodes, key).Upsert(d).Inc(fldSize, inc).Do(ctx)
	if err != nil {
		return fmt.Errorf("error updating node: %v", err)
	}
	return nil
}

func (qs *QuadStore) cleanupNodes(ctx context.Context, keys []Key) error {
	err := qs.db.Delete(colNodes).Keys(keys...).WithFields(FieldFilter{
		Path:   []string{fldSize},
		Filter: Equal,
		Value:  Int(0),
	}).Do(ctx)
	if err != nil {
		err = fmt.Errorf("error cleaning up nodes: %v", err)
	}
	return err
}

func (qs *QuadStore) updateQuad(ctx context.Context, q quad.Quad, proc graph.Procedure) error {
	var setname string
	if proc == graph.Add {
		setname = fldQuadAdded
	} else if proc == graph.Delete {
		setname = fldQuadDeleted
	}
	doc := Document{
		fldSubject:   String(hashOf(q.Subject)),
		fldPredicate: String(hashOf(q.Predicate)),
		fldObject:    String(hashOf(q.Object)),
	}
	if l := hashOf(q.Label); l != "" {
		doc[fldLabel] = String(l)
	}
	err := qs.db.Update(colQuads, getKeyForQuad(q)).Upsert(doc).
		Inc(setname, 1).Do(ctx)
	if err != nil {
		err = fmt.Errorf("quad update failed: %v", err)
	}
	return err
}

func checkQuadValid(q Document) bool {
	added, _ := q[fldQuadAdded].(Int)
	deleted, _ := q[fldQuadDeleted].(Int)
	return added > deleted
}

func (qs *QuadStore) checkValidQuad(ctx context.Context, key Key) (bool, error) {
	q, err := qs.db.FindByKey(ctx, colQuads, key)
	if err == ErrNotFound {
		return false, nil
	}
	if err != nil {
		err = fmt.Errorf("error checking quad validity: %v", err)
		return false, err
	}
	return checkQuadValid(q), nil
}

func (qs *QuadStore) batchInsert(col string) DocWriter {
	return BatchInsert(qs.db, col)
}

func (qs *QuadStore) appendLog(ctx context.Context, deltas []graph.Delta) ([]Key, error) {
	w := qs.batchInsert(colLog)
	defer w.Close()
	for _, d := range deltas {
		data, err := pquads.MakeQuad(d.Quad).Marshal()
		if err != nil {
			return w.Keys(), err
		}
		var action string
		if d.Action == graph.Add {
			action = "AddQuadPQ"
		} else {
			action = "DeleteQuadPQ"
		}
		err = w.WriteDoc(ctx, nil, Document{
			"op":   String(action),
			"data": Bytes(data),
			"ts":   Time(time.Now().UTC()),
		})
		if err != nil {
			return w.Keys(), err
		}
	}
	err := w.Flush(ctx)
	return w.Keys(), err
}

func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta, ignoreOpts graph.IgnoreOpts) error {
	ctx := context.TODO()
	ids := make(map[quad.Value]int)
	// Pre-check the existence condition.
	for _, d := range deltas {
		if d.Action != graph.Add && d.Action != graph.Delete {
			return &graph.DeltaError{Delta: d, Err: graph.ErrInvalidAction}
		}
		valid, err := qs.checkValidQuad(ctx, getKeyForQuad(d.Quad))
		if err != nil {
			return &graph.DeltaError{Delta: d, Err: err}
		}
		switch d.Action {
		case graph.Add:
			if valid {
				if ignoreOpts.IgnoreDup {
					continue
				} else {
					return &graph.DeltaError{Delta: d, Err: graph.ErrQuadExists}
				}
			}
		case graph.Delete:
			if !valid {
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
	if oids, err := qs.appendLog(ctx, deltas); err != nil {
		if i := len(oids); i < len(deltas) {
			return &graph.DeltaError{Delta: deltas[i], Err: err}
		}
		return &graph.DeltaError{Err: err}
	}
	// make sure to create all nodes before writing any quads
	// concurrent reads may observe broken quads in other case
	var gc []Key
	for name, dn := range ids {
		key := qs.nameToKey(name)
		err := qs.updateNodeBy(ctx, key, name, dn)
		if err != nil {
			return err
		}
		if dn < 0 {
			gc = append(gc, key)
		}
	}
	// gc nodes that has negative ref counter
	if err := qs.cleanupNodes(ctx, gc); err != nil {
		return err
	}
	for _, d := range deltas {
		err := qs.updateQuad(ctx, d.Quad, d.Action)
		if err != nil {
			return &graph.DeltaError{Delta: d, Err: err}
		}
	}
	return nil
}

func toDocumentValue(v quad.Value) Document {
	if v == nil {
		return nil
	}
	var doc Document
	switch d := v.(type) {
	case quad.String:
		doc = Document{fldValData: String(d)}
	case quad.Raw:
		doc = Document{fldValData: String(d), fldRaw: Bool(true)}
	case quad.IRI:
		doc = Document{fldValData: String(d), fldIRI: Bool(true)}
	case quad.BNode:
		doc = Document{fldValData: String(d), fldBNode: Bool(true)}
	case quad.TypedString:
		doc = Document{fldValData: String(d.Value), fldType: String(d.Type)}
	case quad.LangString:
		doc = Document{fldValData: String(d.Value), fldLang: String(d.Lang)}
	case quad.Int:
		doc = Document{fldValInt: Int(d)}
	case quad.Float:
		doc = Document{fldValFloat: Float(d)}
	case quad.Bool:
		doc = Document{fldValBool: Bool(d)}
	case quad.Time:
		doc = Document{fldValTime: Time(time.Time(d).UTC())}
	default:
		qv := pquads.MakeValue(v)
		data, err := qv.Marshal()
		if err != nil {
			panic(err)
		}
		doc = Document{fldValPb: Bytes(data)}
	}
	return Document{fldValue: doc}
}

func toQuadValue(d Document) (quad.Value, error) {
	if len(d) == 0 {
		return nil, nil
	}
	var err error
	if v, ok := d[fldValPb]; ok {
		var b []byte
		switch v := v.(type) {
		case String:
			b, err = base64.StdEncoding.DecodeString(string(v))
		case Bytes:
			b = []byte(v)
		default:
			err = fmt.Errorf("unexpected type for pb field: %T", v)
		}
		if err != nil {
			return nil, err
		}
		var p pquads.Value
		if err := p.Unmarshal(b); err != nil {
			return nil, fmt.Errorf("couldn't decode value: %v", err)
		}
		return p.ToNative(), nil
	} else if v, ok := d[fldValInt]; ok {
		var vi quad.Int
		switch v := v.(type) {
		case Int:
			vi = quad.Int(v)
		case Float:
			vi = quad.Int(v)
		default:
			return nil, fmt.Errorf("unexpected type for int field: %T", v)
		}
		return vi, nil
	} else if v, ok := d[fldValFloat]; ok {
		var vf quad.Float
		switch v := v.(type) {
		case Int:
			vf = quad.Float(v)
		case Float:
			vf = quad.Float(v)
		default:
			return nil, fmt.Errorf("unexpected type for float field: %T", v)
		}
		return vf, nil
	} else if v, ok := d[fldValBool]; ok {
		var vb quad.Bool
		switch v := v.(type) {
		case Bool:
			vb = quad.Bool(v)
		default:
			return nil, fmt.Errorf("unexpected type for bool field: %T", v)
		}
		return vb, nil
	} else if v, ok := d[fldValTime]; ok {
		var vt quad.Time
		switch v := v.(type) {
		case Time:
			vt = quad.Time(v)
		case String:
			var t time.Time
			if err := t.UnmarshalJSON([]byte(`"` + string(v) + `"`)); err != nil {
				return nil, err
			}
			vt = quad.Time(t)
		default:
			return nil, fmt.Errorf("unexpected type for bool field: %T", v)
		}
		return vt, nil
	}
	vs, ok := d[fldValData].(String)
	if !ok {
		return nil, fmt.Errorf("unknown value format: %T", d[fldValData])
	}
	if len(d) == 1 {
		return quad.String(vs), nil
	}
	if ok, _ := d[fldIRI].(Bool); ok {
		return quad.IRI(vs), nil
	} else if ok, _ := d[fldBNode].(Bool); ok {
		return quad.BNode(vs), nil
	} else if ok, _ := d[fldRaw].(Bool); ok {
		return quad.Raw(vs), nil
	} else if typ, ok := d[fldType].(String); ok {
		return quad.TypedString{Value: quad.String(vs), Type: quad.IRI(typ)}, nil
	} else if typ, ok := d[fldLang].(String); ok {
		return quad.LangString{Value: quad.String(vs), Lang: string(typ)}, nil
	}
	return nil, fmt.Errorf("unsupported value: %#v", d)
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
	return NewLinksToIterator(qs, "quads", []Linkage{{Dir: d, Val: val.(NodeHash)}})
}

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator(qs, "nodes")
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return NewAllIterator(qs, "quads")
}

func (qs *QuadStore) hashOf(s quad.Value) NodeHash {
	return NodeHash(hashOf(s))
}

func (qs *QuadStore) ValueOf(s quad.Value) graph.Value {
	if s == nil {
		return nil
	}
	return qs.hashOf(s)
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
	nd, err := qs.db.FindByKey(context.TODO(), colNodes, hash.key())
	if err != nil {
		clog.Errorf("couldn't retrieve node %v: %v", v, err)
		return nil
	}
	dv, _ := nd[fldValue].(Document)
	qv, err := toQuadValue(dv)
	if err != nil {
		clog.Errorf("couldn't convert node %v: %v", v, err)
		return nil
	}
	if id, _ := nd[fldHash].(String); id == String(hash) && qv != nil {
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
	var id string
	if v, ok := log[fldLogID].(String); ok {
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
