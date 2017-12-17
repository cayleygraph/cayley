package nosql

import (
	"context"
	"errors"
	"github.com/pborman/uuid"
	"time"
)

var (
	ErrNotFound = errors.New("not found")
)

type Key []string

func (k Key) Value() Value {
	return Strings(k)
}

func GenKey() Key {
	return Key{uuid.NewUUID().String()}
}

func KeyFrom(fields []string, doc Document) Key {
	key := make(Key, 0, len(fields))
	for _, f := range fields {
		if s, ok := doc[f].(String); ok {
			key = append(key, string(s))
		}
	}
	return key
}

type Database interface {
	Insert(ctx context.Context, col string, key Key, d Document) (Key, error)
	FindByKey(ctx context.Context, col string, key Key) (Document, error)
	Query(col string) Query
	Update(col string, key Key) Update
	Delete(col string) Delete
	// EnsureIndex
	//
	// Should create collection if it not exists
	EnsureIndex(ctx context.Context, col string, primary Index, secondary []Index) error
	Close() error
}

type Filter int

const (
	Equal = Filter(iota)
	NotEqual
	GT
	GTE
	LT
	LTE
)

type FieldFilter struct {
	Path   []string
	Filter Filter
	Value  Value
}

type Query interface {
	WithFields(filters ...FieldFilter) Query
	Limit(n int) Query

	Count(ctx context.Context) (int64, error)
	One(ctx context.Context) (Document, error)
	Iterate() DocIterator
}

type Update interface {
	//Set(d Document) Update
	Inc(field string, dn int) Update
	Upsert(d Document) Update
	Do(ctx context.Context) error
}

type Delete interface {
	WithFields(filters ...FieldFilter) Delete
	Keys(keys ...Key) Delete
	Do(ctx context.Context) error
}

type DocIterator interface {
	Next(ctx context.Context) bool
	Err() error
	Close() error
	Key() Key
	Doc() Document
}

// BatchInsert returns a streaming writer for database or emulates it if database has no support for batch inserts.
func BatchInsert(db Database, col string) DocWriter {
	if bi, ok := db.(BatchInserter); ok {
		return bi.BatchInsert(col)
	}
	return &seqInsert{db: db, col: col}
}

type seqInsert struct {
	db   Database
	col  string
	keys []Key
	err  error
}

func (w *seqInsert) WriteDoc(ctx context.Context, key Key, d Document) error {
	key, err := w.db.Insert(ctx, w.col, key, d)
	if err != nil {
		w.err = err
		return err
	}
	w.keys = append(w.keys, key)
	return nil
}

func (w *seqInsert) Flush(ctx context.Context) error {
	return w.err
}

func (w *seqInsert) Keys() []Key {
	return w.keys
}

func (w *seqInsert) Close() error {
	return w.err
}

type DocWriter interface {
	// WriteDoc prepares document to be written. Write becomes valid only after Flush.
	WriteDoc(ctx context.Context, key Key, d Document) error
	// Flush waits for all writes to complete.
	Flush(ctx context.Context) error
	// Keys returns a list of already inserted documents.
	// Might be less then a number of written documents until Flush is called.
	Keys() []Key
	// Close closes writer and discards any unflushed documents.
	Close() error
}

// BatchInserter is an optional interface for databases that can insert documents in batches.
type BatchInserter interface {
	BatchInsert(col string) DocWriter
}

type IndexType int

const (
	IndexAny = IndexType(iota)
	StringExact
	//StringFulltext
	//IntIndex
	//FloatIndex
	//TimeIndex
)

type Index struct {
	Fields []string
	Type   IndexType
}

type Value interface {
	isValue()
}

type Document map[string]Value

func (Document) isValue() {}

type String string

func (String) isValue() {}

type Int int64

func (Int) isValue() {}

type Float float64

func (Float) isValue() {}

type Bool bool

func (Bool) isValue() {}

type Time time.Time

func (Time) isValue() {}

type Bytes []byte

func (Bytes) isValue() {}

type Strings []string

func (Strings) isValue() {}
