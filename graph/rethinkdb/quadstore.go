package rethinkdb

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"gopkg.in/dancannon/gorethink.v2"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/proto"
	"github.com/cayleygraph/cayley/internal/lru"
	"github.com/cayleygraph/cayley/quad"
)

const (
	DefaultDBName = "cayley"
	QuadStoreType = "rethinkdb"
)

const (
	nodeTableName    = "nodes"
	quadTableName    = "quads"
	logTableName     = "log"
	versionTableName = "version"
	version          = 1
)

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

func (NodeHash) IsNode() bool { return true }

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
	session   *gorethink.Session
	ids       *lru.Cache
	sizes     *lru.Cache
	dbVersion int
}

type dbType int

const (
	dbRaw dbType = iota
	dbString
	dbIRI
	dbBNode
	dbFloat
	dbInt
	dbBool
	dbTime
	dbLangString
	dbTypedString
	dbProto
)

type Node struct {
	ID          string    `json:"id"`
	StringValue string    `json:"val_string,omitempty"`
	IntValue    int64     `json:"val_int,omitempty"`
	FloatValue  float64   `json:"val_float,omitempty"`
	TimeValue   time.Time `json:"val_time,omitempty"`
	BoolValue   bool      `json:"val_bool,omitempty"`
	BytesValue  []byte    `json:"val_bytes,omitempty"`
	Type        dbType    `json:"type"`
	LangString  string    `json:"lang_string,omitempty"`
	TypeString  string    `json:"type_string,omitempty"`
	Size        int       `json:"size"`
}

type LogEntry struct {
	ID        string `json:"id"`
	Action    string `json:"action"`
	Key       string `json:"key"`
	Timestamp int64  `json:"ts"`
}

type Quad struct {
	ID        string   `json:"id"`
	Subject   string   `json:"subject"`
	Predicate string   `json:"predicate"`
	Object    string   `json:"object"`
	Label     string   `json:"label"`
	Added     []string `json:"added"`
	Deleted   []string `json:"deleted"`
}

func ensureIndexes(session *gorethink.Session) (err error) {
	// version
	if err = ensureTable(versionTableName, session); err != nil {
		return
	}

	// Insert the current version if the "version" record does not exist
	if err = gorethink.Table(versionTableName).Insert(map[string]interface{}{
		"id":    "version",
		"value": version,
	}, gorethink.InsertOpts{
		Conflict: func(id, oldDoc, newDoc gorethink.Term) interface{} {
			return oldDoc
		},
	}).Exec(session); err != nil {
		return
	}

	// nodes
	if err = ensureTable(nodeTableName, session); err != nil {
		return
	}
	if err = ensureIndex(gorethink.Table(nodeTableName), "type", session); err != nil {
		return
	}

	// quads
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

	// log
	if err = ensureTable(logTableName, session); err != nil {
		return
	}
	if err = ensureIndex(gorethink.Table(logTableName), "key", session); err != nil {
		return
	}

	return
}

func createNewRethinkDBGraph(addr string, options graph.Options) error {
	session, err := dialRethinkDB(addr, options)
	if err != nil {
		return err
	}
	defer session.Close()
	return ensureIndexes(session)
}

func dialRethinkDB(addr string, options graph.Options) (session *gorethink.Session, err error) {
	dbName := DefaultDBName
	if val, ok, err := options.StringKey("database_name"); err == nil && ok {
		dbName = val
	}

	session, err = openSession(addr, dbName)
	return
}

func newQuadStore(addr string, options graph.Options) (qs graph.QuadStore, err error) {
	session, err := dialRethinkDB(addr, options)
	if err != nil {
		return
	}

	if err = ensureIndexes(session); err != nil {
		session.Close()
		return
	}

	var dbVersion = struct {
		Value int `json:"value"`
	}{}
	if err = gorethink.Table(versionTableName).Get("version").ReadOne(&dbVersion, session); err != nil {
		session.Close()
		return
	}

	if dbVersion.Value != version {
		if clog.V(3) {
			clog.Infof("RethinkDB stored version: %d != implementation version: %d", dbVersion.Value, version)
		}
		if err = performUpgrade(dbVersion.Value, session); err != nil {
			session.Close()
			return
		}
	}

	qs = &QuadStore{
		session:   session,
		ids:       lru.New(1 << 16),
		sizes:     lru.New(1 << 16),
		dbVersion: dbVersion.Value,
	}
	return
}

func performUpgrade(dbVersion int, session *gorethink.Session) error {
	// This would be the place to do database upgrades
	if version == 2 && dbVersion == 1 {
		// Do some changes if neccessary ...
	}
	return nil
}

func hashOf(s quad.Value) string {
	if s == nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(quad.HashOf(s))
}

func newQuad(d quad.Quad) Quad {
	h := sha1.New()
	sh := quad.HashOf(d.Subject)
	h.Write(sh)

	ph := quad.HashOf(d.Predicate)
	h.Write(ph)

	oh := quad.HashOf(d.Object)
	h.Write(oh)

	var lh []byte
	var lhs string
	if d.Label != nil {
		lh = quad.HashOf(d.Label)
		lhs = base64.StdEncoding.EncodeToString(lh)
		h.Write(lh)
	}

	return Quad{
		ID:        base64.StdEncoding.EncodeToString(h.Sum(nil)),
		Subject:   base64.StdEncoding.EncodeToString(sh),
		Predicate: base64.StdEncoding.EncodeToString(ph),
		Object:    base64.StdEncoding.EncodeToString(oh),
		Label:     lhs,
	}
}

func newNode(id string, size int, v quad.Value) (n Node) {
	n = Node{
		ID:   id,
		Size: size,
	}

	switch d := v.(type) {
	case nil:
		return
	case quad.Raw:
		n.Type = dbRaw
		n.BytesValue = []byte(d)
	case quad.String:
		n.Type = dbString
		n.StringValue = string(d)
	case quad.IRI:
		n.Type = dbIRI
		n.StringValue = string(d)
	case quad.BNode:
		n.Type = dbBNode
		n.StringValue = string(d)
	case quad.TypedString:
		n.Type = dbTypedString
		n.StringValue = string(d.Value)
		n.TypeString = string(d.Type)
	case quad.LangString:
		n.Type = dbLangString
		n.StringValue = string(d.Value)
		n.LangString = string(d.Lang)
	case quad.Int:
		n.Type = dbInt
		n.IntValue = int64(d)
	case quad.Float:
		n.Type = dbFloat
		n.FloatValue = float64(d)
	case quad.Bool:
		n.Type = dbBool
		n.BoolValue = bool(d)
	case quad.Time:
		n.Type = dbTime
		n.TimeValue = time.Time(d)
	default:
		data, err := proto.MakeValue(v).Marshal()
		if err != nil {
			clog.Errorf("Failed to marshal value")
			return
		}
		n.Type = dbProto
		n.BytesValue = data
	}
	return
}

func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta, ignoreOpts graph.IgnoreOpts) error {
	var logEntries []LogEntry
	var quads []Quad
	var nodes []Node

	for _, d := range deltas {
		quad := newQuad(d.Quad)

		var size int
		if d.Action == graph.Add {
			size = 1
			quad.Added = []string{d.ID.String()}
		} else {
			quad.Deleted = []string{d.ID.String()}
			size = -1
		}

		nodes = append(nodes, newNode(quad.Subject, size, d.Quad.Subject))
		nodes = append(nodes, newNode(quad.Predicate, size, d.Quad.Predicate))
		nodes = append(nodes, newNode(quad.Object, size, d.Quad.Object))

		if d.Quad.Label != nil {
			nodes = append(nodes, newNode(quad.Label, size, d.Quad.Label))
		}

		logEntries = append(logEntries, LogEntry{
			ID:        d.ID.String(),
			Key:       quad.ID,
			Timestamp: d.Timestamp.Unix(),
			Action:    d.Action.String(),
		})
		quads = append(quads, quad)
	}

	// Bulk insert
	if err := gorethink.Table(logTableName).Insert(logEntries).Exec(qs.session); err != nil {
		clog.Errorf("Error updating log: %v", err)
		return err
	}

	if err := gorethink.Table(quadTableName).Insert(quads, gorethink.InsertOpts{
		Conflict: func(_, oldDoc, newDoc gorethink.Term) interface{} {
			return newDoc.Merge(map[string]interface{}{
				"added":   oldDoc.Append(newDoc.Field("added")),
				"deleted": oldDoc.Append(newDoc.Field("deleted")),
			})
		},
	}).Exec(qs.session); err != nil {
		clog.Errorf("Error updating quads: %v", err)
		return err
	}

	if err := gorethink.Table(nodeTableName).Insert(nodes, gorethink.InsertOpts{
		Conflict: func(_, oldDoc, newDoc gorethink.Term) interface{} {
			return newDoc.Merge(map[string]interface{}{
				"size": oldDoc.Add(newDoc.Field("size")),
			})
		},
	}).Exec(qs.session); err != nil {
		clog.Errorf("Error updating nodes: %v", err)
		return err
	}

	return nil
}

func (n Node) quadValue() quad.Value {
	switch n.Type {
	case dbString:
		return quad.String(n.StringValue)
	case dbRaw:
		return quad.Raw(n.BytesValue)
	case dbBNode:
		return quad.BNode(n.StringValue)
	case dbInt:
		return quad.Int(n.IntValue)
	case dbBool:
		return quad.Bool(n.BoolValue)
	case dbFloat:
		return quad.Float(n.FloatValue)
	case dbIRI:
		return quad.IRI(n.StringValue)
	case dbLangString:
		return quad.LangString{
			Value: quad.String(n.StringValue),
			Lang:  n.LangString,
		}
	case dbTypedString:
		return quad.TypedString{
			Value: quad.String(n.StringValue),
			Type:  quad.IRI(n.TypeString),
		}
	case dbTime:
		return quad.Time(n.TimeValue)
	case dbProto:
		var p proto.Value
		if err := p.Unmarshal(n.BytesValue); err != nil {
			clog.Errorf("Error: Couldn't decode value: %v", err)
			return nil
		}
		return p.ToNative()
	default:
		clog.Warningf("Unhandled quad value type: %s", n.Type)
		return nil
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

	var node Node
	query := gorethink.Table(nodeTableName).Get(string(hash))

	if clog.V(5) {
		// Debug
		clog.Infof("Running RDB query: %+v", query)
	}

	err := query.ReadOne(&node, qs.session)
	switch err {
	case nil: // do nothing
	case gorethink.ErrEmptyResult:
		return nil
	default:
		clog.Errorf("Error: Couldn't retrieve node %s %v", v, err)
	}

	qv := node.quadValue()
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
	return in.(QuadHash).Get(d)
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
