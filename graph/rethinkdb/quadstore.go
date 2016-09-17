package rethinkdb

import (
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"sync"
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
	nodeTableName     = "nodes"
	quadTableName     = "quads"
	metadataTableName = "metadata"
	version           = 1
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
	session        *gorethink.Session
	ids            *lru.Cache
	sizes          *lru.Cache
	dbVersion      int
	durabilityMode string // See: https://www.rethinkdb.com/api/javascript/run/
	batchSize      int    // See: https://www.rethinkdb.com/docs/troubleshooting/ (speed up batch writes)
	readMode       string
	maxConnections int
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

type Quad struct {
	ID        string `json:"id"`
	Subject   string `json:"subject"`
	Predicate string `json:"predicate"`
	Object    string `json:"object"`
	Label     string `json:"label"`
}

var dirLinkIndexMap = map[[2]quad.Direction]string{
	{quad.Subject, quad.Predicate}: "subject_predicate",
	{quad.Subject, quad.Object}:    "subject_object",
	{quad.Subject, quad.Label}:     "subject_label",
	{quad.Predicate, quad.Object}:  "predicate_object",
	{quad.Predicate, quad.Label}:   "predicate_label",
	{quad.Object, quad.Label}:      "object_label",
}

func ensureIndexes(session *gorethink.Session) (err error) {
	// version
	if err = ensureTable(metadataTableName, session); err != nil {
		return
	}

	// Insert the current version if the "version" record does not exist
	if err = gorethink.Table(metadataTableName).Insert(map[string]interface{}{
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
	if err = ensureIndexFunc(gorethink.Table(nodeTableName), "val_string",
		func(row gorethink.Term) interface{} {
			return []interface{}{row.Field("type"), row.Field("val_string")}
		}, session); err != nil {
		return
	}
	if err = ensureIndexFunc(gorethink.Table(nodeTableName), "val_int",
		func(row gorethink.Term) interface{} {
			return []interface{}{row.Field("type"), row.Field("val_int")}
		}, session); err != nil {
		return
	}
	if err = ensureIndexFunc(gorethink.Table(nodeTableName), "val_float",
		func(row gorethink.Term) interface{} {
			return []interface{}{row.Field("type"), row.Field("val_float")}
		}, session); err != nil {
		return
	}
	if err = ensureIndexFunc(gorethink.Table(nodeTableName), "val_time",
		func(row gorethink.Term) interface{} {
			return []interface{}{row.Field("type"), row.Field("val_time")}
		}, session); err != nil {
		return
	}
	if err = ensureIndexFunc(gorethink.Table(nodeTableName), "val_bool",
		func(row gorethink.Term) interface{} {
			return []interface{}{row.Field("type"), row.Field("val_bool")}
		}, session); err != nil {
		return
	}
	if err = ensureIndexFunc(gorethink.Table(nodeTableName), "val_bytes",
		func(row gorethink.Term) interface{} {
			return []interface{}{row.Field("type"), row.Field("val_bytes")}
		},
		session); err != nil {
		return
	}
	if err = ensureIndex(gorethink.Table(nodeTableName), "size", session); err != nil {
		return
	}

	// quads
	if err = ensureTable(quadTableName, session); err != nil {
		return
	}
	for _, dir := range []quad.Direction{quad.Subject, quad.Predicate, quad.Object, quad.Label} {
		if err = ensureIndex(gorethink.Table(quadTableName), dir.String(), session); err != nil {
			return
		}
	}
	for key, index := range dirLinkIndexMap {
		if err = ensureIndexFunc(gorethink.Table(quadTableName), index,
			func(row gorethink.Term) interface{} {
				return []interface{}{row.Field(key[0].String()), row.Field(key[1].String())}
			},
			session); err != nil {
			return
		}
	}

	// wait for index
	if err = gorethink.Table(nodeTableName).IndexWait().Exec(session); err != nil {
		return
	}
	if err = gorethink.Table(quadTableName).IndexWait().Exec(session); err != nil {
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

	var username string
	if val, ok, err := options.StringKey("username"); err == nil && ok {
		username = val
	}

	var password string
	if val, ok, err := options.StringKey("password"); err == nil && ok {
		password = val
	}

	maxConnections := 2
	if val, ok, err := options.IntKey("max_connections"); err == nil && ok {
		maxConnections = val
	}

	timeout := time.Duration(time.Second * 2)
	if val, ok, err := options.IntKey("connection_timeout"); err == nil && ok {
		timeout = time.Duration(time.Second * time.Duration(val))
	}

	session, err = openSession(gorethink.ConnectOpts{
		Address:          addr,
		Database:         dbName,
		MaxOpen:          maxConnections,
		HandshakeVersion: gorethink.HandshakeV1_0,
		Username:         username,
		Password:         password,
		Timeout:          timeout,
	}, timeout)
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
	if err = gorethink.Table(metadataTableName).Get("version").ReadOne(&dbVersion, session); err != nil {
		session.Close()
		return
	}

	if dbVersion.Value != version {
		err = fmt.Errorf("RethinkDB version mismatch. DB: %d != impl.: %d", dbVersion.Value, version)
		clog.Errorf("%v", err)
		session.Close()
		return
	}

	durabilityMode := "soft"
	if val, ok, err := options.StringKey("durability_mode"); err == nil && ok {
		durabilityMode = val
	}

	readMode := "single"
	if val, ok, err := options.StringKey("read_mode"); err == nil && ok {
		readMode = val
	}

	batchSize := 200
	if val, ok, err := options.IntKey("batch_size"); err == nil && ok {
		batchSize = val
	}

	maxConnections := 2
	if val, ok, err := options.IntKey("max_connections"); err == nil && ok {
		maxConnections = val
	}

	qs = &QuadStore{
		session:        session,
		ids:            lru.New(1 << 16),
		sizes:          lru.New(1 << 16),
		dbVersion:      dbVersion.Value,
		durabilityMode: durabilityMode,
		batchSize:      batchSize,
		readMode:       readMode,
		maxConnections: maxConnections,
	}
	return
}

func hashOf(s quad.Value) string {
	if s == nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(quad.HashOf(s))
}

func getQuadIDAndHashes(d quad.Quad) (id string, subjectHash, predicateHash, objectHash, labelHash []byte) {
	h := sha1.New()
	subjectHash = quad.HashOf(d.Subject)
	h.Write(subjectHash)

	predicateHash = quad.HashOf(d.Predicate)
	h.Write(predicateHash)

	objectHash = quad.HashOf(d.Object)
	h.Write(objectHash)

	if d.Label != nil {
		labelHash = quad.HashOf(d.Label)
		h.Write(labelHash)
	}

	id = base64.StdEncoding.EncodeToString(h.Sum(nil))
	return
}

func newQuad(d quad.Quad) Quad {
	id, sh, ph, oh, lh := getQuadIDAndHashes(d)
	var lhs string
	if len(lh) > 0 {
		lhs = base64.StdEncoding.EncodeToString(lh)
	}

	return Quad{
		ID:        id,
		Subject:   base64.StdEncoding.EncodeToString(sh),
		Predicate: base64.StdEncoding.EncodeToString(ph),
		Object:    base64.StdEncoding.EncodeToString(oh),
		Label:     lhs,
	}
}

func newNode(id string, v quad.Value) (n Node) {
	n = Node{
		ID:   id,
		Size: 1,
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
			clog.Errorf("Failed to marshal value: %v", err)
			return
		}
		n.Type = dbProto
		n.BytesValue = data
	}
	return
}

func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta, ignoreOpts graph.IgnoreOpts) error {
	var (
		addQuads      []interface{}
		addNodes      []interface{}
		addNodesMap   map[string]Node
		deleteQuadIds []interface{}
		deleteNodeIds []interface{}
	)

	addNodesMap = make(map[string]Node)
	addNode := func(n Node) {
		if v, ok := addNodesMap[n.ID]; ok {
			n.Size += v.Size
		}
		addNodesMap[n.ID] = n
	}

	for _, d := range deltas {
		quad := newQuad(d.Quad)

		if d.Action == graph.Add {
			addQuads = append(addQuads, quad)
			addNode(newNode(quad.Subject, d.Quad.Subject))
			addNode(newNode(quad.Predicate, d.Quad.Predicate))
			addNode(newNode(quad.Object, d.Quad.Object))
			if d.Quad.Label != nil {
				addNode(newNode(quad.Label, d.Quad.Label))
			}
		} else {
			deleteQuadIds = append(deleteQuadIds, quad.ID)
			deleteNodeIds = append(deleteNodeIds, quad.Subject)
			deleteNodeIds = append(deleteNodeIds, quad.Predicate)
			deleteNodeIds = append(deleteNodeIds, quad.Object)
			if d.Quad.Label != nil {
				deleteNodeIds = append(deleteNodeIds, quad.Label)
			}
		}
	}

	// Node array
	for _, n := range addNodesMap {
		addNodes = append(addNodes, n)
	}

	// Batch the queries
	var queries []gorethink.Term

	// Batch delete quad queries
	if len(deleteQuadIds) > 0 {
		for _, batch := range sliceBatch(deleteQuadIds, qs.batchSize) {
			queries = append(queries, gorethink.Table(quadTableName).GetAll(batch...).Delete())
		}

		for _, batch := range sliceBatch(deleteNodeIds, qs.batchSize) {
			// Delete all "unused" nodes
			queries = append(queries, gorethink.Table(nodeTableName).GetAll(batch...).
				Filter(gorethink.Row.Field("size").Le(1)).Delete())
		}
	}

	// Batch add quad queries
	if len(addQuads) > 0 {
		for _, batch := range sliceBatch(addQuads, qs.batchSize) {
			queries = append(queries, gorethink.Table(quadTableName).Insert(batch, gorethink.InsertOpts{
				Conflict: "replace",
			}))
		}

		for _, batch := range sliceBatch(addNodes, qs.batchSize) {
			// Insert nodes
			queries = append(queries, gorethink.Table(nodeTableName).Insert(batch, gorethink.InsertOpts{
				Conflict: func(_, oldDoc, newDoc gorethink.Term) interface{} {
					return newDoc.Merge(map[string]interface{}{
						"size": oldDoc.Add(newDoc.Field("size")),
					})
				},
			}))
		}
	}

	// Run the queries "normal" (no parallelization)
	execNormal := func() error {
		for _, query := range queries {
			if err := query.Exec(qs.session, gorethink.ExecOpts{
				Durability: qs.durabilityMode,
			}); err != nil {
				err = fmt.Errorf("Query failed: %v", err)
				clog.Errorf("%s", err)
				return err
			}
		}
		return nil
	}

	// Run the queries in parallel
	execParallel := func() error {
		var wg sync.WaitGroup
		errCh := make(chan error, 1)
		doneCh := make(chan bool, 1)

		wg.Add(len(queries))
		for _, q := range queries {
			go func(query gorethink.Term) {
				defer wg.Done()
				if err := query.Exec(qs.session, gorethink.ExecOpts{
					Durability: qs.durabilityMode,
				}); err != nil {
					errCh <- err
				}
			}(q)
		}

		go func() {
			wg.Wait()
			close(doneCh)
		}()

		select {
		case <-doneCh:
		case err := <-errCh:
			err = fmt.Errorf("Query failed: %v", err)
			clog.Errorf("%s", err)
			return err
		}

		return nil
	}

	if len(queries) > qs.maxConnections {
		return execParallel()
	}

	return execNormal()
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
		clog.Infof("Running RDB query: %s", query)
	}

	err := query.ReadOne(&node, qs.session, gorethink.RunOpts{
		ReadMode: qs.readMode,
	})
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
		clog.Infof("Running RDB query: %s", query)
	}

	var count int64
	if err := query.ReadOne(&count, qs.session, gorethink.RunOpts{
		ReadMode: qs.readMode,
	}); err != nil {
		clog.Errorf("Error: Couldn't retrieve count: %v", err)
		return 0
	}
	return count
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

func (qs *QuadStore) getSize(query gorethink.Term) (size int64, err error) {
	size = -1

	query = query.Count()

	key := string(query.String())
	if val, ok := qs.sizes.Get(key); ok {
		size = val.(int64)
		return
	}

	if clog.V(5) {
		clog.Infof("Running RDB query: %s", query)
	}

	if err = query.ReadOne(&size, qs.session, gorethink.RunOpts{
		ReadMode: qs.readMode,
	}); err != nil {
		clog.Errorf("Failed to get size for iterator: %v", err)
		return
	}

	if clog.V(5) {
		clog.Infof("Got size for iterator: %d (%s)", size, key)
	}

	qs.sizes.Put(key, size)
	return
}
