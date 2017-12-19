package ouch

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"runtime"
	"strings"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/nosql"

	"github.com/go-kivik/kivik"
)

const Type = "ouch"

func init() {
	nosql.Register(Type, nosql.Registration{
		NewFunc:      Open,
		InitFunc:     Create,
		IsPersistent: true,
	})
}

func dialDB(create bool, addr string, opt graph.Options) (*DB, error) {
	ctx := context.TODO() // TODO - replace with parameter value

	driver := defaultDriverName

	addrParsed, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}

	pathParts := strings.Split(addrParsed.Path, "/")
	dbName := ""
	if len(pathParts) > 0 && addr != "" {
		dbName = pathParts[len(pathParts)-1]
	} else {
		return nil, errors.New("unable to decypher database name from: " + addr)
	}
	dsn := strings.TrimSuffix(addr, dbName)

	client, err := kivik.New(ctx, driver, dsn)
	if err != nil {
		return nil, err
	}

	if create {
		err := client.CreateDB(ctx, dbName)
		if err != nil {
			return nil, err
		}
	}

	db, err := client.DB(ctx, dbName)
	if err != nil {
		return nil, err
	}

	return &DB{
		db:     db,
		colls:  make(map[string]collection),
		driver: driver,
	}, nil
}

func Create(addr string, opt graph.Options) (nosql.Database, error) {
	return dialDB(true, addr, opt)
}

func Open(addr string, opt graph.Options) (nosql.Database, error) {
	return dialDB(false, addr, opt)
}

type collection struct {
	primary   nosql.Index
	secondary []nosql.Index
}

type DB struct {
	db     *kivik.DB
	colls  map[string]collection
	driver string
}

func (db *DB) Close() error {
	// seems no way to close a kivik session, so just let the Go garbage collection do its stuff...
	return nil
}

const (
	collectionIndex   = "collection-index"
	secondaryIndexFmt = "%s-secondary-%d"
)

func (db *DB) EnsureIndex(ctx context.Context, col string, primary nosql.Index, secondary []nosql.Index) error {

	if primary.Type != nosql.StringExact {
		return fmt.Errorf("unsupported type of primary index: %v", primary.Type)
	}

	db.colls[col] = collection{
		primary:   primary,
		secondary: secondary,
	}

	if err := db.db.CreateIndex(ctx, collectionIndex, collectionIndex,
		map[string]interface{}{
			"fields": []string{collectionField}, //  collection field only, default index
		}); err != nil {
		return err
	}

	// NOTE the field of the primary index is always "_id", so need not create an index

	for k, v := range db.colls[col].secondary {
		snam := fmt.Sprintf(secondaryIndexFmt, col, k)
		sindex := map[string]interface{}{
			"fields": v.Fields,
		}
		if err := db.db.CreateIndex(ctx, snam, snam, sindex); err != nil {
			return err
		}
	}

	return nil
}

const (
	idField         = "_id"
	revField        = "_rev"
	collectionField = "Collection"
)

func compKey(col string, key nosql.Key) string {
	return "K" + strings.Join(key, "|")
}

func (db *DB) Insert(ctx context.Context, col string, key nosql.Key, d nosql.Document) (nosql.Key, error) {
	k, _, e := db.insert(ctx, col, key, d)
	return k, e
}
func (db *DB) insert(ctx context.Context, col string, key nosql.Key, d nosql.Document) (nosql.Key, string, error) {

	if d == nil {
		return nil, "", errors.New("no document to insert")
	}

	rev := ""
	if key == nil {
		key = nosql.GenKey()
	} else {
		var err error
		var id string
		_, id, rev, err = db.findByKey(ctx, col, key)
		if err == nil {
			rev, err = db.db.Delete(ctx, id, rev) // delete it to be sure it is removed
			if err != nil {
				return nil, "", err
			}
		}
	}

	if cP, found := db.colls[col]; found {
		// go through the key list an put each in
		if len(cP.primary.Fields) == len(key) {
			for idx, nam := range cP.primary.Fields {
				d[nam] = nosql.String(key[idx]) // just replace with the given key, even if there already
			}
		}
	}

	interfaceDoc := toOuchDoc(col, toOuchValue(idField, key.Value()).(string), rev, d)

	_, rev, err := db.db.CreateDoc(ctx, interfaceDoc)
	if err != nil {
		return nil, "", err
	}

	return key, rev, nil
}

func (db *DB) FindByKey(ctx context.Context, col string, key nosql.Key) (nosql.Document, error) {
	decoded, _, _, err := db.findByKey(ctx, col, key)
	return decoded, err
}

func (db *DB) findByKey(ctx context.Context, col string, key nosql.Key) (nosql.Document, string, string, error) {
	cK := compKey(col, key)
	return db.findByOuchKey(ctx, cK)
}

func (db *DB) findByOuchKey(ctx context.Context, cK string) (nosql.Document, string, string, error) {

	row, err := db.db.Get(ctx, cK)
	if err != nil {
		if kivik.StatusCode(err) == kivik.StatusNotFound {
			return nil, "", "", nosql.ErrNotFound
		}
		return nil, "", "", err
	}

	rowDoc := make(map[string]interface{})
	err = row.ScanDoc(&rowDoc)
	if err != nil {
		return nil, "", "", err
	}
	decoded := fromOuchDoc(rowDoc)

	return decoded, rowDoc[idField].(string), rowDoc[revField].(string), nil
}

func (db *DB) Query(col string) nosql.Query {
	qry := &Query{
		db:          db,
		col:         col,
		pathFilters: make(map[string][]nosql.FieldFilter),
		ouchQuery: map[string]interface{}{
			"selector": map[string]interface{}{},
			"limit":    1000000, // million row limit, default is 25 TODO is 1M enough?
		},
	}
	if col != "" {
		qry.ouchQuery["selector"].(map[string]interface{})[collectionField] = col
	}
	return qry
}
func (db *DB) Update(col string, key nosql.Key) nosql.Update {
	return &Update{db: db, col: col, key: key, update: nosql.Document{}}
}
func (db *DB) Delete(col string) nosql.Delete {
	return &Delete{db: db, col: col, q: db.Query(col).(*Query)}
}

type ouchQuery map[string]interface{}

type Query struct {
	db          *DB
	col         string
	pathFilters map[string][]nosql.FieldFilter
	ouchQuery
}

func (q *Query) WithFields(filters ...nosql.FieldFilter) nosql.Query {
	for _, filter := range filters {
		j := strings.Join(filter.Path, keySeparator)
		q.pathFilters[j] = append(q.pathFilters[j], filter)
	}

	return q
}

func (q *Query) buildFilters() nosql.Query {
	for jp, filterList := range q.pathFilters {
		term := map[string]interface{}{}
		for _, filter := range filterList {
			test := ""
			testValue := toOuchValue(".", filter.Value)
			if stringValue, isString := testValue.(string); isString && len(stringValue) > 0 {
				typeChar := stringValue[0]
				typeCharNext := typeChar + 1
				switch filter.Filter {
				case nosql.Equal, nosql.NotEqual:
				// nothing to do as not a relative test
				case nosql.LT, nosql.LTE:
					term["$gte"] = string(typeChar) // set the lower bound
				case nosql.GT, nosql.GTE:
					term["$lt"] = string(typeCharNext) // set the upper bound
				default:
					// panic in the switch below
				}
			}
			switch filter.Filter {
			case nosql.Equal:
				test = "$eq"
			case nosql.NotEqual:
				if boolVal, isBool := testValue.(bool); isBool && boolVal && runtime.GOARCH != "js" {
					// Swap the logic of the test, which is required to make it work for missing values in CouchDB.
					// Sadly, this same formulation does not work for PouchDB, as that does not allow $or.
					test = "$or"
					testValue = []interface{}{
						map[string]interface{}{"$eq": false},     // it was $ne true
						map[string]interface{}{"$exists": false}, // non-existence => false
					}
				} else {
					test = "$ne"
				}
			case nosql.GT:
				test = "$gt"
			case nosql.GTE:
				test = "$gte"
			case nosql.LT:
				test = "$lt"
			case nosql.LTE:
				test = "$lte"
			default:
				msg := fmt.Sprintf("unknown nosqlFilter %v", filter.Filter)
				fmt.Println(msg)
				panic(msg)
			}
			term[test] = testValue
		}
		q.ouchQuery["selector"].(map[string]interface{})[jp] = term
	}

	if len(q.pathFilters) == 0 {
		q.ouchQuery["use_index"] = collectionIndex
	} else {
		// NOTE primary is redundant, as the same as the _id
		c, haveCol := q.db.colls[q.col]
		if haveCol {
			for si, sv := range c.secondary {
				useSecondary := true
				for _, fieldName := range sv.Fields {
					if _, found := q.pathFilters[fieldName]; !found {
						useSecondary = false
					}
				}
				if useSecondary {
					q.ouchQuery["use_index"] = fmt.Sprintf(secondaryIndexFmt, q.col, si)
					break
				}
			}
		}
	}

	return q
}

func (q *Query) Limit(n int) nosql.Query {
	q.ouchQuery["limit"] = n
	return q
}

func (q *Query) Count(ctx context.Context) (int64, error) {
	// TODO it shoud be possible to use map/reduce logic, rather than a mango query, to speed this up, at least for some cases

	// don't pull back any fields in the query, to reduce bandwidth
	q.ouchQuery["fields"] = []interface{}{}

	it := q.Iterate().(*Iterator)
	//defer it.Close() // closed automatically at the last Next()
	var count int64
	for it.rows.Next() { // for speed, use the native Next
		count++
	}
	return count, it.err
}

func (q *Query) One(ctx context.Context) (nosql.Document, error) {
	it := q.Iterate()
	defer it.Close()
	if err := it.Err(); err != nil {
		return nil, err
	}
	if it.Next(ctx) {
		return it.Doc(), it.(*Iterator).err
	}
	return nil, nosql.ErrNotFound
}

func (q *Query) Iterate() nosql.DocIterator {
	ctx := context.TODO() // TODO - replace with parameter value
	q.buildFilters()

	// NOTE: to see that the query actually is using an index, uncomment the lines below
	// queryPlan, err := q.db.db.Explain(ctx, q.ouchQuery)
	// debug := fmt.Sprintf("DEBUG %v QueryPlan: %#v", err, queryPlan)
	// if strings.Contains(debug, collectionIndex) { // put the index you're interested in here
	// 	fmt.Println(debug)
	// }

	rows, err := q.db.db.Find(ctx, q.ouchQuery)
	return &Iterator{rows: rows, err: err}
}

type Iterator struct {
	err     error
	rows    *kivik.Rows
	doc     map[string]interface{}
	hadNext bool
	closed  bool
}

func (it *Iterator) Next(ctx context.Context) bool {
	it.hadNext = true
	if it.err != nil || it.closed {
		return false
	}
	it.doc = nil
	haveNext := it.rows.Next()
	it.err = it.rows.Err()
	if it.err != nil {
		return false
	}
	if haveNext {
		it.scanDoc()
		if it.err != nil {
			return false
		}
	} else {
		it.closed = true // auto-closed at end of iteration by API
	}
	return haveNext
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Close() error {
	if it.err == nil && !it.closed {
		it.err = it.rows.Close()
	}
	it.closed = true
	return it.err
}

func (it *Iterator) Key() nosql.Key {
	if it.err != nil || it.closed {
		return nil
	}
	if !it.hadNext {
		it.err = errors.New("call to Iterator.Key before Iterator.Next")
		return nil
	}

	id, haveID := it.doc[idField].(string)
	if !haveID {
		it.err = fmt.Errorf("Iterator.Key ID empty")
		return nil
	}
	ret := nosql.Key([]string(fromOuchValue("?", id).(nosql.Strings)))
	return ret
}

func (it *Iterator) Doc() nosql.Document {
	if it.err != nil || it.closed {
		return nil
	}
	if !it.hadNext {
		it.err = errors.New("Iterator.Doc called before Iterator.Next")
		return nil
	}
	return fromOuchDoc(it.doc)
}

func (it *Iterator) scanDoc() {
	if it.doc == nil && it.err == nil && !it.closed {
		it.doc = map[string]interface{}{}
		it.err = it.rows.ScanDoc(&it.doc)
	}
}

type Delete struct {
	db   *DB
	col  string
	q    *Query
	keys []interface{}
}

func (d *Delete) WithFields(filters ...nosql.FieldFilter) nosql.Delete {
	d.q.WithFields(filters...)
	return d
}
func (d *Delete) Keys(keys ...nosql.Key) nosql.Delete {
	for _, k := range keys {
		id := toOuchValue("?", k.Value()).(string)
		d.keys = append(d.keys, id)
	}
	return d
}
func (d *Delete) Do(ctx context.Context) error {

	deleteSet := make(map[string]string) // [_id]_rev

	switch len(d.keys) {
	case 0:
	// no keys to test against
	case 1:
		if len(d.q.pathFilters) == 0 {
			// this special case is optimised not to use the query/iterate route at all,
			// but rather to fetch the _id and _rev directly from the given key.
			_, id, rev, err := d.db.findByOuchKey(ctx, d.keys[0].(string))
			if err != nil {
				return err
			}
			deleteSet[id] = rev
		} else {
			d.q.ouchQuery["selector"].(map[string]interface{})[idField] = map[string]interface{}{"$eq": d.keys[0]}
		}

	default:
		d.q.ouchQuery["selector"].(map[string]interface{})[idField] = map[string]interface{}{"$in": d.keys}
	}

	// NOTE even when using idField in a Mango query, it still has to base its query on an index

	if len(deleteSet) == 0 { // did not hit the special case, so must do a mango query

		// only pull back the _id & _rev fields in the query
		d.q.ouchQuery["fields"] = []interface{}{idField, revField}

		it := d.q.Iterate().(*Iterator)
		if it.Err() != nil {
			return it.Err()
		}

		for it.Next(ctx) {
			id := it.doc[idField].(string)
			rev := it.doc[revField].(string)
			deleteSet[id] = rev
		}
		if it.err != nil {
			return it.err
		}
		if err := it.Close(); err != nil {
			return err
		}
	}

	for id, rev := range deleteSet {
		_, err := d.db.db.Delete(ctx, id, rev)
		if err != nil {
			return err
		}
	}

	return nil
}

type Update struct {
	db     *DB
	col    string
	key    nosql.Key
	update nosql.Document
	upsert bool
	inc    map[string]int // increment the named numeric field by the int
}

func (u *Update) Inc(field string, dn int) nosql.Update {
	if u.inc == nil {
		u.inc = make(map[string]int)
	}
	u.inc[field] += dn
	return u
}

func (u *Update) Upsert(d nosql.Document) nosql.Update {
	u.upsert = true
	for k, v := range d {
		u.update[k] = v
	}
	return u
}
func (u *Update) Do(ctx context.Context) error {
	orig, id, rev, err := u.db.findByKey(ctx, u.col, u.key)
	if err == nosql.ErrNotFound {
		if !u.upsert {
			return err
		}
		var idKey nosql.Key
		orig = u.update
		idKey, rev, err = u.db.insert(ctx, u.col, u.key, orig)
		if err != nil {
			return err
		}
		id = toOuchValue(idField, idKey.Value()).(string)
	} else {
		if err != nil {
			return err
		}
		for k, v := range u.update { // alter any changed fields
			orig[k] = v
		}
	}

	for k, v := range u.inc { // increment numerical values
		val, exists := orig[k]
		if exists {
			switch x := val.(type) {
			case nosql.Int:
				val = nosql.Int(int64(x) + int64(v))
			case nosql.Float:
				val = nosql.Float(float64(x) + float64(v))
			default:
				return errors.New("field '" + k + "' is not a number")
			}
		} else {
			val = nosql.Int(v)
		}
		orig[k] = val
	}

	_, err = u.db.db.Put(ctx, compKey(u.col, u.key), toOuchDoc(u.col, id, rev, orig))
	return err
}
