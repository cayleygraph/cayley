package rethinkdb

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"gopkg.in/dancannon/gorethink.v2"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/proto"
	"github.com/cayleygraph/cayley/internal/lru"
	"github.com/cayleygraph/cayley/quad"
)

const DefaultDBName = "cayley"
const QuadStoreType = "rethinkdb"
const nodeTableName = "nodes"
const quadTableName = "quads"
const logTableName = "log"

func init() {
	graph.RegisterQuadStore(QuadStoreType, graph.QuadStoreRegistration{
		NewFunc:           newQuadStore,
		NewForRequestFunc: nil,
		UpgradeFunc:       nil,
		InitFunc:          createNewRethinkDBGraph,
		IsPersistent:      true,
	})
}

type NodeHash string

func (NodeHash) IsNode() bool { return false }

type QuadHash [4]NodeHash

func (QuadHash) IsNode() bool { return false }

func (q QuadHash) Get(d quad.Direction) NodeHash {
	switch d {
	case quad.Subject:
		return q[0]
	case quad.Predicate:
		return q[1]
	case quad.Object:
		return q[2]
	case quad.Label:
		return q[3]
	}
	panic(fmt.Errorf("unknown direction: %v", d))
}

type QuadStore struct {
	session *gorethink.Session
	ids     *lru.Cache
	sizes   *lru.Cache
}

type RethinkDBNode struct {
	ID   string `json:"id"`
	Name value  `json:"name"`
	Size int    `json:"size"`
}

type RethinkDBLogEntry struct {
	ID        string `json:"id"`
	Action    string `json:"action"`
	Key       string `json:"key"`
	Timestamp int64  `json:"ts"`
}

type value interface{}

type RethinkDBQuad struct {
	ID        string   `json:"id"`
	Subject   string   `json:"subject"`
	Predicate string   `json:"predicate"`
	Object    string   `json:"object"`
	Label     string   `json:"label"`
	Added     []string `json:"added"`
	Deleted   []string `json:"deleted"`
}

type RethinkDBString struct {
	Value   string `json:"val"`
	IsIRI   bool   `json:"iri,omitempty"`
	IsBNode bool   `json:"bnode,omitempty"`
	Type    string `json:"type,omitempty"`
	Lang    string `json:"lang,omitempty"`
}

func ensureIndexes(session *gorethink.Session) (err error) {
	if err = ensureTable(nodeTableName, session); err != nil {
		return
	}
	if err = ensureTable(quadTableName, session); err != nil {
		return
	}
	if err = ensureIndex(gorethink.Table(quadTableName), "subject", session); err != nil {
		return
	}
	if err = ensureIndex(gorethink.Table(quadTableName), "predicate", session); err != nil {
		return
	}
	if err = ensureIndex(gorethink.Table(quadTableName), "object", session); err != nil {
		return
	}
	if err = ensureIndex(gorethink.Table(quadTableName), "label", session); err != nil {
		return
	}

	if err = ensureTable(logTableName, session); err != nil {
		return
	}
	if err = ensureIndex(gorethink.Table(logTableName), "key", session); err != nil {
		return
	}

	return
}

func createNewRethinkDBGraph(addr string, options graph.Options) (err error) {
	session, err := dialRethinkDB(addr, options)
	if err != nil {
		return
	}
	return ensureIndexes(session)
}

func dialRethinkDB(addr string, options graph.Options) (session *gorethink.Session, err error) {
	dbName := DefaultDBName
	if val, ok, err := options.StringKey("database_name"); err == nil && ok {
		dbName = val
	}

	if val, ok, err := options.StringKey("log_level"); err == nil && ok {
		switch strings.ToUpper(val) {
		case "DEBUG":
			clog.SetV(5)
		case "INFO":
			clog.SetV(4)
		case "WARN":
			clog.SetV(3)
		case "ERROR":
			clog.SetV(2)
		case "FATAL":
			clog.SetV(1)
		}
	}

	session, err = openSession(addr, dbName)
	if err != nil {
		return
	}
	return
}

func newQuadStore(addr string, options graph.Options) (qs graph.QuadStore, err error) {
	s := QuadStore{}
	session, err := dialRethinkDB(addr, options)
	if err != nil {
		return
	}

	if err = ensureIndexes(session); err != nil {
		return
	}
	s.session = session
	s.ids = lru.New(1 << 16)
	s.sizes = lru.New(1 << 16)
	qs = &s
	return
}

func hashOf(s quad.Value) string {
	if s == nil {
		return ""
	}
	return hex.EncodeToString(quad.HashOf(s))
}

func (qs *QuadStore) getIDForQuad(t quad.Quad) string {
	hash := hashOf(t.Subject)
	hash += hashOf(t.Predicate)
	hash += hashOf(t.Object)
	hash += hashOf(t.Label)
	h := sha1.New()
	h.Write([]byte(hash))
	return hex.EncodeToString(h.Sum(nil))
}

func (qs *QuadStore) updateNodeBy(name quad.Value, inc int) (err error) {
	node := qs.ValueOf(name)

	query := gorethink.Table(nodeTableName).Insert(RethinkDBNode{
		ID:   string(node.(NodeHash)),
		Name: toRethinkDBValue(name),
		Size: inc,
	}, gorethink.InsertOpts{
		Conflict: "replace",
	})

	if clog.V(5) {
		// Debug
		clog.Infof("Running RDB query: %+v", query)
	}

	if err = query.Exec(qs.session); err != nil {
		clog.Errorf("Error updating node: %v", err)
		return
	}

	return
}

func (qs *QuadStore) updateQuad(q quad.Quad, id string, proc graph.Procedure) (err error) {
	row := RethinkDBQuad{
		ID:        qs.getIDForQuad(q),
		Subject:   hashOf(q.Subject),
		Predicate: hashOf(q.Predicate),
		Object:    hashOf(q.Object),
		Label:     hashOf(q.Label),
	}

	var setname string
	switch proc {
	case graph.Add:
		setname = "added"
		row.Added = []string{id}
	case graph.Delete:
		row.Deleted = []string{id}
		setname = "deleted"
	}

	query := gorethink.Table(quadTableName).Insert(row, gorethink.InsertOpts{
		Conflict: func(_, oldDoc, newDoc gorethink.Term) interface{} {
			return newDoc.Merge(map[string]interface{}{
				setname: oldDoc.Append(newDoc.Field(setname)),
			})
		},
	})

	if clog.V(5) {
		// Debug
		clog.Infof("Running RDB query: %+v", query)
	}

	if err = query.Exec(qs.session); err != nil {
		clog.Errorf("Error updating quad: %v", err)
		return
	}

	return
}

func (qs *QuadStore) updateLog(d graph.Delta) (err error) {
	var action string
	if d.Action == graph.Add {
		action = "Add"
	} else {
		action = "Delete"
	}

	query := gorethink.Table(logTableName).Insert(&RethinkDBLogEntry{
		ID:        d.ID.String(),
		Action:    action,
		Key:       qs.getIDForQuad(d.Quad),
		Timestamp: d.Timestamp.UnixNano(),
	})

	if clog.V(5) {
		// Debug
		clog.Infof("Running RDB query: %+v", query)
	}

	if err = query.Exec(qs.session); err != nil {
		clog.Errorf("Error updating log: %v", err)
		return
	}

	return
}

func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta, ignoreOpts graph.IgnoreOpts) error {
	ids := make(map[quad.Value]int)

	for _, d := range deltas {
		err := qs.updateLog(d)
		if err != nil {
			return &graph.DeltaError{Delta: d, Err: err}
		}
	}
	for _, d := range deltas {
		err := qs.updateQuad(d.Quad, d.ID.String(), d.Action)
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

	return nil
}

func toRethinkDBValue(v quad.Value) value {
	if v == nil {
		return nil
	}
	switch d := v.(type) {
	case quad.Raw:
		return string(d) // compatibility
	case quad.String:
		return RethinkDBString{Value: string(d)}
	case quad.IRI:
		return RethinkDBString{Value: string(d), IsIRI: true}
	case quad.BNode:
		return RethinkDBString{Value: string(d), IsBNode: true}
	case quad.TypedString:
		return RethinkDBString{Value: string(d.Value), Type: string(d.Type)}
	case quad.LangString:
		return RethinkDBString{Value: string(d.Value), Lang: string(d.Lang)}
	case quad.Int:
		return int64(d)
	case quad.Float:
		return float64(d)
	case quad.Bool:
		return bool(d)
	case quad.Time:
		return time.Time(d)
	default:
		qv := proto.MakeValue(v)
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
	case map[string]interface{}:
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
	h := val.(QuadHash)
	return quad.Quad{
		Subject:   qs.NameOf(NodeHash(h.Get(quad.Subject))),
		Predicate: qs.NameOf(NodeHash(h.Get(quad.Predicate))),
		Object:    qs.NameOf(NodeHash(h.Get(quad.Object))),
		Label:     qs.NameOf(NodeHash(h.Get(quad.Label))),
	}
}

func (qs *QuadStore) QuadIterator(d quad.Direction, val graph.Value) graph.Iterator {
	return NewIterator(qs, quadTableName, d, val)
}

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator(qs, nodeTableName)
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return NewAllIterator(qs, quadTableName)
}

func (qs *QuadStore) ValueOf(s quad.Value) graph.Value {
	return NodeHash(hashOf(s))
}

func (qs *QuadStore) NameOf(v graph.Value) quad.Value {
	hash := v.(NodeHash)
	if hash == "" {
		return nil
	}
	if val, ok := qs.ids.Get(string(hash)); ok {
		return val.(quad.Value)
	}
	var node RethinkDBNode

	query := gorethink.Table(nodeTableName).Get(string(hash))

	if clog.V(5) {
		// Debug
		clog.Infof("Running RDB query: %+v", query)
	}

	err := query.ReadOne(&node, qs.session)
	switch err {
	case nil: // do nothing
	case gorethink.ErrEmptyResult: // do nothing
	default:
		clog.Errorf("Error: Couldn't retrieve node %s %v", v, err)
	}

	qv := toQuadValue(node.Name)
	if node.ID != "" && qv != nil {
		qs.ids.Put(string(hash), qv)
	}
	return qv
}

func (qs *QuadStore) Size() int64 {
	query := gorethink.Table(quadTableName).Count()

	if clog.V(5) {
		// Debug
		clog.Infof("Running RDB query: %+v", query)
	}

	var count int
	if err := query.ReadOne(&count, qs.session); err != nil {
		clog.Errorf("Error: Couldn't retrieve count: %v", err)
		return 0
	}
	return int64(count)
}

func (qs *QuadStore) Horizon() graph.PrimaryKey {
	return graph.NewUniqueKey("")
}

func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(iterator.Identity)
}

func (qs *QuadStore) Close() {
	qs.session.Close()
}

func (qs *QuadStore) QuadDirection(in graph.Value, d quad.Direction) graph.Value {
	return NodeHash(in.(QuadHash).Get(d))
}

func (qs *QuadStore) Type() string {
	return QuadStoreType
}

func (qs *QuadStore) getSize(collection string, constraint *gorethink.Term) (size int64, err error) {
	size = -1
	bytes, err := json.Marshal(constraint)
	if err != nil {
		clog.Errorf("Couldn't marshal internal constraint")
		return
	}
	key := collection + string(bytes)
	if val, ok := qs.sizes.Get(key); ok {
		size = val.(int64)
		return
	}

	var query gorethink.Term

	if constraint == nil {
		query = gorethink.Table(collection).Count()
	} else {
		query = gorethink.Table(collection).Filter(*constraint).Count()
	}

	if clog.V(5) {
		// Debug
		clog.Infof("Running RDB query: %+v", query)
	}

	if err = query.ReadOne(&size, qs.session); err != nil {
		clog.Errorf("Trouble getting size for iterator! %v", err)
		return
	}
	qs.sizes.Put(key, size)
	return
}
