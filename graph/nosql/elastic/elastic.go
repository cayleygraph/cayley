package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"gopkg.in/olivere/elastic.v5"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/nosql"
)

const Type = "elastic"

var (
	_ nosql.BatchInserter = (*DB)(nil)
)

func init() {
	nosql.Register(Type, nosql.Registration{
		NewFunc:      Open,
		InitFunc:     Create,
		IsPersistent: true,
	})
}

// dialElastic connects to elasticsearch
func dialElastic(addr string, options graph.Options) (*elastic.Client, error) {
	client, err := elastic.NewClient(elastic.SetURL(addr))
	if err != nil {
		return nil, err
	}
	return client, nil
}

func dialDB(addr string, opt graph.Options) (*DB, error) {
	client, err := dialElastic(addr, opt)
	if err != nil {
		return nil, err
	}
	ind, _, err := opt.StringKey("index")
	if err != nil {
		return nil, err
	} else if ind == "" {
		ind = nosql.DefaultDBName
	}
	settings := `{
			"number_of_shards":1,
			"number_of_replicas":0
		}`
	switch o := opt["settings"].(type) {
	case string:
		settings = o
	}
	return &DB{
		cli: client, ind: ind, indSettings: json.RawMessage(settings),
		colls: make(map[string]collection),
	}, nil
}

func Create(addr string, opt graph.Options) (nosql.Database, error) {
	return dialDB(addr, opt)
}

func Open(addr string, opt graph.Options) (nosql.Database, error) {
	return dialDB(addr, opt)
}

type collection struct {
	typ       string
	compPK    bool // compose PK from existing keys; if false, use id instead of target field
	primary   nosql.Index
	secondary []nosql.Index
}

type DB struct {
	cli         *elastic.Client
	ind         string
	indSettings json.RawMessage
	colls       map[string]collection
}

func (db *DB) Close() error {
	db.cli.CloseIndex(db.ind)
	return nil
}

type indType string

const (
	indKeyword = indType("keyword")
)

type property struct {
	Type indType `json:"type"`
}

func (db *DB) EnsureIndex(ctx context.Context, typ string, primary nosql.Index, secondary []nosql.Index) error {
	if primary.Type != nosql.StringExact {
		return fmt.Errorf("unsupported type of primary index: %v", primary.Type)
	}
	compPK := len(primary.Fields) > 1

	exists := true
	conf, err := db.cli.GetMapping().Index(db.ind).Do(ctx)
	if e, ok := err.(*elastic.Error); ok && e.Status == 404 {
		exists = false
	} else if err != nil {
		return err
	}
	conf, _ = conf[db.ind].(map[string]interface{})

	mappings, _ := conf["mappings"].(map[string]interface{})
	if mappings == nil {
		mappings = make(map[string]interface{})
	}

	props := make(map[string]property)
	if compPK {
		for _, f := range primary.Fields {
			props[f] = property{Type: indKeyword}
		}
	}
	for _, ind := range secondary {
		for _, f := range ind.Fields {
			if _, ok := props[f]; ok {
				continue
			}
			var typ indType
			switch ind.Type {
			case nosql.StringExact:
				typ = indKeyword
			}
			if typ != "" {
				props[f] = property{Type: typ}
			}
		}
	}
	cur := map[string]interface{}{"properties": props}
	mappings[typ] = cur

	if conf == nil {
		conf = make(map[string]interface{})
	}
	if _, ok := conf["settings"]; !ok {
		conf["settings"] = db.indSettings
	}
	conf["mappings"] = mappings

	if !exists {
		_, err = db.cli.CreateIndex(db.ind).BodyJson(conf).Do(ctx)
	} else {
		_, err = db.cli.PutMapping().Index(db.ind).Type(typ).BodyJson(cur).Do(ctx)
	}
	if err != nil {
		return err
	}
	db.colls[typ] = collection{
		typ:       typ,
		compPK:    compPK,
		primary:   primary,
		secondary: secondary,
	}
	return nil
}
func toElasticValue(v nosql.Value) interface{} {
	switch v := v.(type) {
	case nil:
		return nil
	case nosql.Document:
		return toElasticDoc(v)
	case nosql.Strings:
		return []string(v)
	case nosql.String:
		return string(v)
	case nosql.Int:
		return int64(v)
	case nosql.Float:
		return float64(v)
	case nosql.Bool:
		return bool(v)
	case nosql.Time:
		return time.Time(v)
	case nosql.Bytes:
		return []byte(v)
	default:
		panic(fmt.Errorf("unsupported type: %T", v))
	}
}
func fromElasticValue(v interface{}) nosql.Value {
	switch v := v.(type) {
	case nil:
		return nil
	case map[string]interface{}:
		return fromElasticDoc(v)
	case []interface{}:
		arr := make(nosql.Strings, 0, len(v))
		for _, s := range v {
			sv := fromElasticValue(s)
			str, ok := sv.(nosql.String)
			if !ok {
				panic(fmt.Errorf("unsupported value in array: %T", sv))
			}
			arr = append(arr, string(str))
		}
		return arr
	case string:
		return nosql.String(v)
	case json.Number:
		if vi, err := v.Int64(); err == nil {
			return nosql.Int(vi)
		}
		vf, _ := v.Float64()
		return nosql.Float(vf)
	case int:
		return nosql.Int(v)
	case int64:
		return nosql.Int(v)
	case float64:
		return nosql.Float(v)
	case bool:
		return nosql.Bool(v)
	case time.Time:
		return nosql.Time(v)
	case []byte:
		return nosql.Bytes(v)
	default:
		panic(fmt.Errorf("unsupported type: %T", v))
	}
}
func toElasticDoc(d nosql.Document) map[string]interface{} {
	if d == nil {
		return nil
	}
	m := make(map[string]interface{}, len(d))
	for k, v := range d {
		m[k] = toElasticValue(v)
	}
	return m
}
func fromElasticDoc(d map[string]interface{}) nosql.Document {
	if d == nil {
		return nil
	}
	m := make(nosql.Document, len(d))
	for k, v := range d {
		m[k] = fromElasticValue(v)
	}
	return m
}

func (c *collection) getKey(h *elastic.SearchHit) nosql.Key {
	if !c.compPK {
		return nosql.Key{h.Id}
	}
	d := c.convDoc(h)
	// key field computed from multiple source fields
	// get source fields from document in correct order
	key := make(nosql.Key, 0, len(c.primary.Fields))
	for _, f := range c.primary.Fields {
		s, _ := d[f].(nosql.String)
		key = append(key, string(s))
	}
	return key
}

func (c *collection) setKey(m map[string]interface{}, key nosql.Key) {
	if !c.compPK {
		// delete source field, since we already added it as _id
		delete(m, c.primary.Fields[0])
	} else {
		for i, f := range c.primary.Fields {
			m[f] = string(key[i])
		}
	}
}

func (c *collection) convDoc(h *elastic.SearchHit) nosql.Document {
	var m map[string]interface{}
	dec := json.NewDecoder(bytes.NewReader(*h.Source))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		panic(err)
	}
	if !c.compPK {
		// key field renamed - set correct name
		m[c.primary.Fields[0]] = string(h.Id)
	}
	return fromElasticDoc(m)
}

func (c *collection) convIns(key nosql.Key, d nosql.Document) (string, map[string]interface{}) {
	mid := compKey(key)
	m := toElasticDoc(d)
	c.setKey(m, key)
	return mid, m
}

func compKey(key nosql.Key) string {
	if len(key) == 1 {
		return key[0]
	}
	return strings.Join(key, "|")
}

func (db *DB) Insert(ctx context.Context, col string, key nosql.Key, d nosql.Document) (nosql.Key, error) {
	if key == nil {
		key = nosql.GenKey()
	}
	c, ok := db.colls[col]
	if !ok {
		return nil, fmt.Errorf("collection %q not found", col)
	}
	mid, m := c.convIns(key, d)
	if _, err := db.cli.Index().Index(db.ind).Type(col).Id(mid).BodyJson(m).Do(ctx); err != nil {
		return nil, err
	}
	if _, err := db.cli.Flush(db.ind).Do(ctx); err != nil {
		return nil, err
	}
	return key, nil
}
func (db *DB) FindByKey(ctx context.Context, col string, key nosql.Key) (nosql.Document, error) {
	c := db.colls[col]
	resp, err := db.cli.Search(db.ind).Type(col).Query(
		elastic.NewIdsQuery(col).Ids(compKey(key)),
	).Size(1).Do(ctx)
	if err != nil {
		return nil, err
	} else if resp.TotalHits() == 0 {
		return nil, nosql.ErrNotFound
	}
	h := resp.Hits.Hits[0]
	return c.convDoc(h), nil
}
func (db *DB) indexRef(col string) indexRef {
	c := db.colls[col]
	return indexRef{cli: db.cli, ind: db.ind, c: &c}
}
func (db *DB) Query(col string) nosql.Query {
	return &Query{indexRef: db.indexRef(col)}
}
func (db *DB) Update(col string, key nosql.Key) nosql.Update {
	return &Update{indexRef: db.indexRef(col), key: key}
}
func (db *DB) Delete(col string) nosql.Delete {
	return &Delete{indexRef: db.indexRef(col)}
}

type elasticQuery struct {
	Keys    []nosql.Key
	Filters []nosql.FieldFilter
}

func (q elasticQuery) IsAll() bool {
	return len(q.Keys) == 0 && len(q.Filters) == 0
}

func (q elasticQuery) Source() (interface{}, error) {
	type rng struct {
		GTE interface{} `json:"gte,omitempty"`
		GT  interface{} `json:"gt,omitempty"`
		LTE interface{} `json:"lte,omitempty"`
		LT  interface{} `json:"lt,omitempty"`
	}
	term := func(name string, v interface{}) map[string]interface{} {
		return map[string]interface{}{
			"term": map[string]interface{}{
				name: v,
			},
		}
	}
	var filters, must, not []map[string]interface{}
	if len(q.Keys) != 0 {
		var ids []string
		for _, k := range q.Keys {
			ids = append(ids, compKey(k))
		}
		must = append(must, map[string]interface{}{
			"ids": map[string][]string{
				"values": ids,
			},
		})
	}
	ranges := make(map[string]rng)
	for _, f := range q.Filters {
		name := strings.Join(f.Path, ".")
		val := toElasticValue(f.Value)
		switch f.Filter {
		case nosql.Equal:
			filters = append(filters, term(name, toElasticValue(f.Value)))
		case nosql.NotEqual:
			not = append(not, term(name, toElasticValue(f.Value)))
		case nosql.GT, nosql.GTE, nosql.LT, nosql.LTE:
			r := ranges[name]
			switch f.Filter {
			case nosql.GT:
				r.GT = val
			case nosql.GTE:
				r.GTE = val
			case nosql.LT:
				r.LT = val
			case nosql.LTE:
				r.LTE = val
			default:
				panic("unreachable")
			}
			ranges[name] = r
		default:
			return nil, fmt.Errorf("unsupported filter: %v", f.Filter)
		}
	}
	if len(ranges) != 0 {
		for name, r := range ranges {
			must = append(must, map[string]interface{}{
				"range": map[string]interface{}{
					name: r,
				},
			})
		}
	}
	qbool := make(map[string]interface{}, 3)
	if len(filters) != 0 {
		qbool["filter"] = filters
	}
	if len(must) != 0 {
		qbool["must"] = must
	}
	if len(not) != 0 {
		qbool["must_not"] = not
	}
	return map[string]interface{}{
		"bool": qbool,
	}, nil
}

type indexRef struct {
	cli *elastic.Client
	ind string
	c   *collection
}

type Query struct {
	indexRef
	limit int64
	qu    elasticQuery
}

func (q *Query) WithFields(filters ...nosql.FieldFilter) nosql.Query {
	q.qu.Filters = append(q.qu.Filters, filters...)
	return q
}
func (q *Query) Limit(n int) nosql.Query {
	q.limit = int64(n)
	return q
}
func (q *Query) Count(ctx context.Context) (int64, error) {
	cnt := q.cli.Count(q.ind).Type(q.c.typ)
	if !q.qu.IsAll() {
		cnt = cnt.Query(q.qu)
	}
	n, err := cnt.Do(ctx)
	if err != nil {
		return 0, err
	}
	if q.limit > 0 && n > q.limit {
		n = q.limit
	}
	return n, nil
}
func (q *Query) One(ctx context.Context) (nosql.Document, error) {
	qu := q.cli.Search(q.ind).Type(q.c.typ).Size(1)
	if !q.qu.IsAll() {
		qu = qu.Query(q.qu)
	}
	resp, err := qu.Do(ctx)
	if err != nil {
		return nil, err
	} else if len(resp.Hits.Hits) == 0 {
		return nil, nosql.ErrNotFound
	}
	return q.c.convDoc(resp.Hits.Hits[0]), nil
}
func (q *Query) Iterate() nosql.DocIterator {
	qu := q.cli.Scroll(q.ind).Type(q.c.typ)
	if q.limit > 0 {
		qu = qu.Size(int(q.limit))
	}
	if !q.qu.IsAll() {
		qu = qu.Query(q.qu)
	}
	return &Iterator{indexRef: q.indexRef, qu: qu}
}

type Iterator struct {
	indexRef
	qu *elastic.ScrollService

	buf  *elastic.SearchResult
	done bool
	i    int
	err  error
}

func (it *Iterator) Next(ctx context.Context) bool {
	if it.done {
		return false
	}
	if it.buf == nil {
		it.buf, it.err = it.qu.Do(ctx)
	} else if it.i+1 >= len(it.buf.Hits.Hits) {
		it.i = 0
		it.buf, it.err = it.cli.Scroll(it.ind).ScrollId(it.buf.ScrollId).Do(ctx)
	} else {
		it.i++
	}
	if it.err == io.EOF {
		it.err = nil
		it.done = true
	}
	if it.err != nil || it.done || it.i >= len(it.buf.Hits.Hits) {
		it.done = true
		return false
	}
	return true
}
func (it *Iterator) Err() error {
	return it.err
}
func (it *Iterator) Close() error {
	return nil
}
func (it *Iterator) hit() *elastic.SearchHit {
	if it.buf == nil || it.i >= len(it.buf.Hits.Hits) {
		return nil
	}
	return it.buf.Hits.Hits[it.i]
}
func (it *Iterator) Key() nosql.Key {
	h := it.hit()
	if h == nil {
		return nil
	}
	return it.c.getKey(h)
}
func (it *Iterator) Doc() nosql.Document {
	h := it.hit()
	if h == nil {
		return nil
	}
	return it.c.convDoc(h)
}

type Delete struct {
	indexRef
	qu elasticQuery
}

func (d *Delete) WithFields(filters ...nosql.FieldFilter) nosql.Delete {
	d.qu.Filters = append(d.qu.Filters, filters...)
	return d
}
func (d *Delete) Keys(keys ...nosql.Key) nosql.Delete {
	if len(keys) == 0 {
		return d
	}
	d.qu.Keys = append(d.qu.Keys, keys...)
	return d
}
func (d *Delete) Do(ctx context.Context) error {
	del := d.cli.DeleteByQuery(d.ind).Type(d.c.typ)
	if !d.qu.IsAll() {
		del = del.Query(d.qu)
	}
	_, err := del.Do(ctx)
	if err != nil {
		return err
	}
	_, err = d.cli.Flush(d.ind).Do(ctx)
	return err
}

type Update struct {
	indexRef
	key nosql.Key

	upsert map[string]interface{}
	inc    map[string]int
}

func (u *Update) Inc(field string, dn int) nosql.Update {
	if u.inc == nil {
		u.inc = make(map[string]int)
	}
	u.inc[field] = u.inc[field] + dn
	return u
}
func (u *Update) Upsert(d nosql.Document) nosql.Update {
	u.upsert = toElasticDoc(d)
	if u.upsert == nil {
		u.upsert = make(map[string]interface{})
	}
	u.c.setKey(u.upsert, u.key)
	return u
}
func (u *Update) Do(ctx context.Context) error {
	upd := u.cli.Update().Index(u.ind).Type(u.c.typ).Id(compKey(u.key))
	if len(u.inc) != 0 {
		var script []string
		if u.upsert == nil {
			u.upsert = make(map[string]interface{})
		}
		for f, dn := range u.inc {
			script = append(script, fmt.Sprintf("ctx._source.%s = (ctx._source.%s ?: 0) %+d", f, f, dn))
			u.upsert[f] = dn
		}
		upd = upd.Script(elastic.NewScript(strings.Join(script, "\n")))
	} else {
		// either doc or script should be set, so we will set doc
		doc := make(map[string]interface{})
		u.c.setKey(doc, u.key)
		upd = upd.Doc(doc)
	}
	if len(u.upsert) != 0 {
		upd = upd.Upsert(u.upsert)
	}
	_, err := upd.Do(ctx)
	if err != nil {
		return err
	}
	_, err = u.cli.Flush(u.ind).Do(ctx)
	return err
}

func (db *DB) BatchInsert(col string) nosql.DocWriter {
	return &inserter{indexRef: db.indexRef(col)}
}

const batchSize = 100

type inserter struct {
	indexRef
	buf   []elastic.BulkableRequest
	ikeys []nosql.Key
	keys  []nosql.Key
	err   error
}

func (w *inserter) WriteDoc(ctx context.Context, key nosql.Key, d nosql.Document) error {
	if len(w.buf) >= batchSize {
		if err := w.Flush(ctx); err != nil {
			return err
		}
	}
	if key == nil {
		key = nosql.GenKey()
	}
	mid, m := w.c.convIns(key, d)
	w.buf = append(w.buf, elastic.NewBulkIndexRequest().Id(mid).Doc(m))
	w.ikeys = append(w.ikeys, key)
	return nil
}

func (w *inserter) Flush(ctx context.Context) error {
	if len(w.buf) == 0 {
		return w.err
	}
	_, err := w.cli.Bulk().Index(w.ind).Type(w.c.typ).Add(w.buf...).Do(ctx)
	if err == nil {
		_, err = w.cli.Flush(w.ind).Do(ctx)
	}
	if err != nil {
		w.err = err
		return err
	}
	w.keys = append(w.keys, w.ikeys...)
	w.ikeys = w.ikeys[:0]
	w.buf = w.buf[:0]
	return w.err
}

func (w *inserter) Keys() []nosql.Key {
	return w.keys
}

func (w *inserter) Close() error {
	w.ikeys = nil
	w.buf = nil
	return w.err
}
