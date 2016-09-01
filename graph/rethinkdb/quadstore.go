package rethinkdb

import (
	"crypto/sha1"
	"encoding/hex"
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
	nodeTableName = "nodes"
	quadTableName = "quads"
	logTableName  = "log"
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

type QuadID [sha1.Size]byte

type NodeHash [quad.HashSize]byte

func (NodeHash) IsNode() bool { return true }

func (h NodeHash) Valid() bool {
	return h != NodeHash{}
}

func (h NodeHash) toSlice() []byte {
	if !h.Valid() {
		return nil
	}
	return []byte(h[:])
}
func (h NodeHash) String() string {
	if !h.Valid() {
		return ""
	}
	return hex.EncodeToString(h[:])
}

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

// quadID is the combined hash for the quad (subject, predicate, object, [label])
func (q QuadHash) quadID() QuadID {
	h := sha1.New()
	h.Write(q[0].toSlice())
	h.Write(q[1].toSlice())
	h.Write(q[2].toSlice())
	if q[3].Valid() {
		h.Write(q[3].toSlice())
	}
	var r QuadID
	copy(r[:], h.Sum(nil))
	return r
}

func (q QuadHash) subject() NodeHash {
	return q[0]
}

func (q QuadHash) predicate() NodeHash {
	return q[1]
}

func (q QuadHash) object() NodeHash {
	return q[2]
}

func (q QuadHash) label() NodeHash {
	return q[3]
}

type QuadStore struct {
	session *gorethink.Session
	ids     *lru.Cache
	sizes   *lru.Cache
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
	ID          NodeHash  `json:"id"`
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
	Key       QuadID `json:"key"`
	Timestamp int64  `json:"ts"`
}

type Quad struct {
	ID        QuadID   `json:"id"`
	Subject   NodeHash `json:"subject"`
	Predicate NodeHash `json:"predicate"`
	Object    NodeHash `json:"object"`
	Label     NodeHash `json:"label"`
	Added     []string `json:"added"`
	Deleted   []string `json:"deleted"`
}

func (q Quad) quadHash() QuadHash {
	return QuadHash{
		q.Subject,
		q.Predicate,
		q.Object,
		q.Label,
	}
}

func nodeHashOf(s quad.Value) (out NodeHash) {
	if s == nil {
		return
	}
	quad.HashTo(s, out[:])
	return
}

func quadHashOf(q quad.Quad) (h QuadHash) {
	h[0] = nodeHashOf(q.Subject)
	h[1] = nodeHashOf(q.Predicate)
	h[2] = nodeHashOf(q.Object)
	if q.Label != nil {
		h[3] = nodeHashOf(q.Label)
	}
	return
}

func ensureIndexes(session *gorethink.Session) (err error) {
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

	qs = &QuadStore{
		session: session,
		ids:     lru.New(1 << 16),
		sizes:   lru.New(1 << 16),
	}
	return
}

func (qs *QuadStore) updateNodeBy(name quad.Value, inc int) (err error) {
	node := qs.ValueOf(name)

	row := &Node{
		ID:   node.(NodeHash),
		Size: inc,
	}

	row.fillValue(name)

	query := gorethink.Table(nodeTableName).Insert(*row, gorethink.InsertOpts{
		Conflict: func(_, oldDoc, newDoc gorethink.Term) interface{} {
			return newDoc.Merge(map[string]interface{}{
				"size": oldDoc.Add(newDoc.Field("size")),
			})
		},
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
	qh := quadHashOf(q)

	row := Quad{
		ID:        qh.quadID(),
		Subject:   qh.subject(),
		Predicate: qh.predicate(),
		Object:    qh.object(),
		Label:     qh.label(),
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

	qh := quadHashOf(d.Quad)

	query := gorethink.Table(logTableName).Insert(LogEntry{
		ID:        d.ID.String(),
		Action:    action,
		Key:       qh.quadID(),
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

func (n *Node) fillValue(v quad.Value) {
	if v == nil {
		return
	}
	switch d := v.(type) {
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
		n.TimeValue = time.Time(d).UTC()
	default:
		qv := proto.MakeValue(v)
		data, err := qv.Marshal()
		if err != nil {
			panic(err)
		}
		n.Type = dbProto
		n.BytesValue = data
	}
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
		return quad.Time(n.TimeValue.UTC())
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
		Subject:   qs.NameOf(h.Get(quad.Subject)),
		Predicate: qs.NameOf(h.Get(quad.Predicate)),
		Object:    qs.NameOf(h.Get(quad.Object)),
		Label:     qs.NameOf(h.Get(quad.Label)),
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
	return nodeHashOf(s)
}

func (qs *QuadStore) NameOf(v graph.Value) quad.Value {
	hash := v.(NodeHash)
	if !hash.Valid() {
		return nil
	}
	if val, ok := qs.ids.Get(hash.String()); ok {
		return val.(quad.Value)
	}

	var node Node
	query := gorethink.Table(nodeTableName).Get(hash)

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
	if node.ID.Valid() && qv != nil {
		qs.ids.Put(hash.String(), qv)
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
