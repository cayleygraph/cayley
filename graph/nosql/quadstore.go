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
	"fmt"
	"time"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/internal/lru"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/pquads"
	"github.com/hidal-go/hidalgo/legacy/nosql"
)

const DefaultDBName = "cayley"

type Registration struct {
	NewFunc      NewFunc
	InitFunc     InitFunc
	IsPersistent bool
	Traits
}

type Traits = nosql.Traits

func init() {
	for _, reg := range nosql.List() {
		Register(reg.Name, Registration{
			NewFunc: func(addr string, options graph.Options) (nosql.Database, error) {
				return reg.Open(addr, DefaultDBName, nosql.Options(options))
			},
			InitFunc: func(addr string, options graph.Options) (nosql.Database, error) {
				return reg.New(addr, DefaultDBName, nosql.Options(options))
			},
			IsPersistent: !reg.Volatile, Traits: reg.Traits,
		})
	}
}

type InitFunc func(string, graph.Options) (nosql.Database, error)
type NewFunc func(string, graph.Options) (nosql.Database, error)

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
			nopt := r.Traits
			qs, err := NewQuadStore(db, &nopt, opt)
			if err != nil {
				return nil, err
			}
			return qs, nil
		},
		IsPersistent: r.IsPersistent,
	})
}

func Init(db nosql.Database, opt graph.Options) error {
	return ensureIndexes(context.TODO(), db)
}

func NewQuadStore(db nosql.Database, nopt *Traits, opt graph.Options) (*QuadStore, error) {
	if err := ensureIndexes(context.TODO(), db); err != nil {
		return nil, err
	}
	qs := &QuadStore{
		db:    db,
		ids:   lru.New(1 << 16),
		sizes: lru.New(1 << 16),
	}
	if nopt != nil {
		qs.opt = *nopt
	}
	return qs, nil
}

type NodeHash string

func (NodeHash) IsNode() bool       { return false }
func (v NodeHash) Key() interface{} { return v }
func (v NodeHash) key() nosql.Key   { return nosql.Key{string(v)} }

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

	fldValData   = "str"
	fldIRI       = "iri"
	fldBNode     = "bnode"
	fldType      = "type"
	fldLang      = "lang"
	fldValInt    = "int"
	fldValStrInt = "int_str"
	fldValFloat  = "float"
	fldValBool   = "bool"
	fldValTime   = "ts"
	fldValPb     = "pb"
)

type QuadStore struct {
	db    nosql.Database
	ids   *lru.Cache
	sizes *lru.Cache
	opt   Traits
}

func ensureIndexes(ctx context.Context, db nosql.Database) error {
	err := db.EnsureIndex(ctx, colLog, nosql.Index{
		Fields: []string{fldLogID},
		Type:   nosql.StringExact,
	}, nil)
	if err != nil {
		return err
	}
	err = db.EnsureIndex(ctx, colNodes, nosql.Index{
		Fields: []string{fldHash},
		Type:   nosql.StringExact,
	}, nil)
	if err != nil {
		return err
	}
	err = db.EnsureIndex(ctx, colQuads, nosql.Index{
		Fields: []string{
			fldSubject,
			fldPredicate,
			fldObject,
			fldLabel,
		},
		Type: nosql.StringExact,
	}, []nosql.Index{
		{Fields: []string{fldSubject}, Type: nosql.StringExact},
		{Fields: []string{fldPredicate}, Type: nosql.StringExact},
		{Fields: []string{fldObject}, Type: nosql.StringExact},
		{Fields: []string{fldLabel}, Type: nosql.StringExact},
	})
	if err != nil {
		return err
	}
	return nil
}

func getKeyForQuad(t quad.Quad) nosql.Key {
	return nosql.Key{
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
	h := quad.HashOf(s)
	return base64.StdEncoding.EncodeToString(h)
}

func (qs *QuadStore) nameToKey(name quad.Value) nosql.Key {
	node := qs.hashOf(name)
	return node.key()
}

func (qs *QuadStore) updateNodeBy(ctx context.Context, key nosql.Key, name quad.Value, inc int) error {
	if inc == 0 {
		return nil
	}
	d := toDocumentValue(&qs.opt, name)
	err := qs.db.Update(colNodes, key).Upsert(d).Inc(fldSize, inc).Do(ctx)
	if err != nil {
		return fmt.Errorf("error updating node: %v", err)
	}
	return nil
}

func (qs *QuadStore) cleanupNodes(ctx context.Context, keys []nosql.Key) error {
	err := qs.db.Delete(colNodes).Keys(keys...).WithFields(nosql.FieldFilter{
		Path:   []string{fldSize},
		Filter: nosql.Equal,
		Value:  nosql.Int(0),
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
	doc := nosql.Document{
		fldSubject:   nosql.String(hashOf(q.Subject)),
		fldPredicate: nosql.String(hashOf(q.Predicate)),
		fldObject:    nosql.String(hashOf(q.Object)),
	}
	if l := hashOf(q.Label); l != "" {
		doc[fldLabel] = nosql.String(l)
	}
	err := qs.db.Update(colQuads, getKeyForQuad(q)).Upsert(doc).
		Inc(setname, 1).Do(ctx)
	if err != nil {
		err = fmt.Errorf("quad update failed: %v", err)
	}
	return err
}

func checkQuadValid(q nosql.Document) bool {
	added, _ := asInt(q[fldQuadAdded])
	deleted, _ := asInt(q[fldQuadDeleted])
	return added > deleted
}

func (qs *QuadStore) checkValidQuad(ctx context.Context, key nosql.Key) (bool, error) {
	q, err := qs.db.FindByKey(ctx, colQuads, key)
	if err == nosql.ErrNotFound {
		return false, nil
	}
	if err != nil {
		err = fmt.Errorf("error checking quad validity: %v", err)
		return false, err
	}
	return checkQuadValid(q), nil
}

func (qs *QuadStore) batchInsert(col string) nosql.DocWriter {
	return nosql.BatchInsert(qs.db, col)
}

func (qs *QuadStore) appendLog(ctx context.Context, deltas []graph.Delta) ([]nosql.Key, error) {
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
		err = w.WriteDoc(ctx, nil, nosql.Document{
			"op":   nosql.String(action),
			"data": nosql.Bytes(data),
			"ts":   nosql.Time(time.Now().UTC()),
		})
		if err != nil {
			return w.Keys(), err
		}
	}
	err := w.Flush(ctx)
	return w.Keys(), err
}

func (qs *QuadStore) NewQuadWriter() (quad.WriteCloser, error) {
	return &quadWriter{qs: qs}, nil
}

type quadWriter struct {
	qs     *QuadStore
	deltas []graph.Delta
}

func (w *quadWriter) WriteQuad(q quad.Quad) error {
	_, err := w.WriteQuads([]quad.Quad{q})
	return err
}

func (w *quadWriter) WriteQuads(buf []quad.Quad) (int, error) {
	// TODO(dennwc): write an optimized implementation
	w.deltas = w.deltas[:0]
	if cap(w.deltas) < len(buf) {
		w.deltas = make([]graph.Delta, 0, len(buf))
	}
	for _, q := range buf {
		w.deltas = append(w.deltas, graph.Delta{
			Quad: q, Action: graph.Add,
		})
	}
	err := w.qs.ApplyDeltas(w.deltas, graph.IgnoreOpts{
		IgnoreDup: true,
	})
	w.deltas = w.deltas[:0]
	if err != nil {
		return 0, err
	}
	return len(buf), nil
}

func (w *quadWriter) Close() error {
	w.deltas = nil
	return nil
}

func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta, ignoreOpts graph.IgnoreOpts) error {
	ctx := context.TODO()
	ids := make(map[quad.Value]int)

	var validDeltas []graph.Delta
	if ignoreOpts.IgnoreDup || ignoreOpts.IgnoreMissing {
		validDeltas = make([]graph.Delta, 0, len(deltas))
	}
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
		if validDeltas != nil {
			validDeltas = append(validDeltas, d)
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
	if validDeltas != nil {
		deltas = validDeltas
	}
	if oids, err := qs.appendLog(ctx, deltas); err != nil {
		if i := len(oids); i < len(deltas) {
			return &graph.DeltaError{Delta: deltas[i], Err: err}
		}
		return &graph.DeltaError{Err: err}
	}
	// make sure to create all nodes before writing any quads
	// concurrent reads may observe broken quads in other case
	var gc []nosql.Key
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

func toDocumentValue(opt *Traits, v quad.Value) nosql.Document {
	if v == nil {
		return nil
	}
	var doc nosql.Document
	encPb := func() {
		qv := pquads.MakeValue(v)
		data, err := qv.Marshal()
		if err != nil {
			panic(err)
		}
		doc[fldValPb] = nosql.Bytes(data)
	}
	switch d := v.(type) {
	case quad.String:
		doc = nosql.Document{fldValData: nosql.String(d)}
	case quad.IRI:
		doc = nosql.Document{fldValData: nosql.String(d), fldIRI: nosql.Bool(true)}
	case quad.BNode:
		doc = nosql.Document{fldValData: nosql.String(d), fldBNode: nosql.Bool(true)}
	case quad.TypedString:
		doc = nosql.Document{fldValData: nosql.String(d.Value), fldType: nosql.String(d.Type)}
	case quad.LangString:
		doc = nosql.Document{fldValData: nosql.String(d.Value), fldLang: nosql.String(d.Lang)}
	case quad.Int:
		doc = nosql.Document{fldValInt: nosql.Int(d)}
		if opt.Number32 {
			// store sortable string representation for range queries
			doc[fldValStrInt] = nosql.String(itos(int64(d)))
			encPb()
		}
	case quad.Float:
		doc = nosql.Document{fldValFloat: nosql.Float(d)}
		if opt.Number32 {
			encPb()
		}
	case quad.Bool:
		doc = nosql.Document{fldValBool: nosql.Bool(d)}
	case quad.Time:
		doc = nosql.Document{fldValTime: nosql.Time(time.Time(d).UTC())}
	default:
		encPb()
	}
	return nosql.Document{fldValue: doc}
}

func asInt(v nosql.Value) (nosql.Int, error) {
	var vi nosql.Int
	switch v := v.(type) {
	case nosql.Int:
		vi = v
	case nosql.Float:
		vi = nosql.Int(v)
	default:
		return 0, fmt.Errorf("unexpected type for int field: %T", v)
	}
	return vi, nil
}

func toQuadValue(opt *Traits, d nosql.Document) (quad.Value, error) {
	if len(d) == 0 {
		return nil, nil
	}
	var err error
	// prefer protobuf representation
	if v, ok := d[fldValPb]; ok {
		var b []byte
		switch v := v.(type) {
		case nosql.String:
			b, err = base64.StdEncoding.DecodeString(string(v))
		case nosql.Bytes:
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
		if opt.Number32 {
			// parse from string, so we are confident that we will get exactly the same value
			if vs, ok := d[fldValStrInt].(nosql.String); ok {
				iv := quad.Int(stoi(string(vs)))
				return iv, nil
			}
		}
		vi, err := asInt(v)
		if err != nil {
			return nil, err
		}
		return quad.Int(vi), nil
	} else if v, ok := d[fldValFloat]; ok {
		var vf quad.Float
		switch v := v.(type) {
		case nosql.Int:
			vf = quad.Float(v)
		case nosql.Float:
			vf = quad.Float(v)
		default:
			return nil, fmt.Errorf("unexpected type for float field: %T", v)
		}
		return vf, nil
	} else if v, ok := d[fldValBool]; ok {
		var vb quad.Bool
		switch v := v.(type) {
		case nosql.Bool:
			vb = quad.Bool(v)
		default:
			return nil, fmt.Errorf("unexpected type for bool field: %T", v)
		}
		return vb, nil
	} else if v, ok := d[fldValTime]; ok {
		var vt quad.Time
		switch v := v.(type) {
		case nosql.Time:
			vt = quad.Time(v)
		case nosql.String:
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
	vs, ok := d[fldValData].(nosql.String)
	if !ok {
		return nil, fmt.Errorf("unknown value format: %T", d[fldValData])
	}
	if len(d) == 1 {
		return quad.String(vs), nil
	}
	if ok, _ := d[fldIRI].(nosql.Bool); ok {
		return quad.IRI(vs), nil
	} else if ok, _ := d[fldBNode].(nosql.Bool); ok {
		return quad.BNode(vs), nil
	} else if typ, ok := d[fldType].(nosql.String); ok {
		return quad.TypedString{Value: quad.String(vs), Type: quad.IRI(typ)}, nil
	} else if typ, ok := d[fldLang].(nosql.String); ok {
		return quad.LangString{Value: quad.String(vs), Lang: string(typ)}, nil
	}
	return nil, fmt.Errorf("unsupported value: %#v", d)
}

func (qs *QuadStore) Quad(val graph.Ref) quad.Quad {
	h := val.(QuadHash)
	return quad.Quad{
		Subject:   qs.NameOf(NodeHash(h.Get(quad.Subject))),
		Predicate: qs.NameOf(NodeHash(h.Get(quad.Predicate))),
		Object:    qs.NameOf(NodeHash(h.Get(quad.Object))),
		Label:     qs.NameOf(NodeHash(h.Get(quad.Label))),
	}
}

func (qs *QuadStore) QuadIterator(d quad.Direction, val graph.Ref) graph.Iterator {
	h, ok := val.(NodeHash)
	if !ok {
		return iterator.NewNull()
	}
	return NewLinksToIterator(qs, "quads", []Linkage{{Dir: d, Val: h}})
}

func (qs *QuadStore) QuadIteratorSize(ctx context.Context, d quad.Direction, v graph.Ref) (graph.Size, error) {
	h, ok := v.(NodeHash)
	if !ok {
		return graph.Size{Size: 0, Exact: true}, nil
	}
	sz, err := qs.getSize("quads", linkageToFilters([]Linkage{{Dir: d, Val: h}}))
	if err != nil {
		return graph.Size{}, err
	}
	return graph.Size{
		Size:  sz,
		Exact: true,
	}, nil
}

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return NewIterator(qs, "nodes")
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return NewIterator(qs, "quads")
}

func (qs *QuadStore) hashOf(s quad.Value) NodeHash {
	return NodeHash(hashOf(s))
}

func (qs *QuadStore) ValueOf(s quad.Value) graph.Ref {
	if s == nil {
		return nil
	}
	return qs.hashOf(s)
}

func (qs *QuadStore) NameOf(v graph.Ref) quad.Value {
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
	dv, _ := nd[fldValue].(nosql.Document)
	qv, err := toQuadValue(&qs.opt, dv)
	if err != nil {
		clog.Errorf("couldn't convert node %v: %v", v, err)
		return nil
	}
	if id, _ := nd[fldHash].(nosql.String); id == nosql.String(hash) && qv != nil {
		qs.ids.Put(string(hash), qv)
	}
	return qv
}

func (qs *QuadStore) Stats(ctx context.Context, exact bool) (graph.Stats, error) {
	// TODO(barakmich): Make size real; store it in the log, and retrieve it.
	nodes, err := qs.db.Query(colNodes).Count(ctx)
	if err != nil {
		return graph.Stats{}, err
	}
	quads, err := qs.db.Query(colQuads).Count(ctx)
	if err != nil {
		return graph.Stats{}, err
	}
	return graph.Stats{
		Nodes: graph.Size{
			Size:  nodes,
			Exact: true,
		},
		Quads: graph.Size{
			Size:  quads,
			Exact: true,
		},
	}, nil
}

func (qs *QuadStore) Size() int64 {
	count, err := qs.db.Query(colQuads).Count(context.TODO())
	if err != nil {
		clog.Errorf("%v", err)
		return 0
	}
	return count
}

func (qs *QuadStore) Close() error {
	return qs.db.Close()
}

func (qs *QuadStore) QuadDirection(in graph.Ref, d quad.Direction) graph.Ref {
	return NodeHash(in.(QuadHash).Get(d))
}

func (qs *QuadStore) getSize(col string, constraints []nosql.FieldFilter) (int64, error) {
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
