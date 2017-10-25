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
