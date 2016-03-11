package quad

import (
	"crypto/sha1"
	"hash"
	"strconv"
	"sync"
	"time"
)

// Value is a type used by all quad directions.
type Value interface {
	String() string
}

// Equaler interface is implemented by values, that needs a special equality check.
type Equaler interface {
	Equal(v Value) bool
}

// HashSize is a size of the slice, returned by HashOf.
const HashSize = sha1.Size

var hashPool = sync.Pool{
	New: func() interface{} { return sha1.New() },
}

// HashOf calculates a hash of value v.
func HashOf(v Value) []byte {
	key := make([]byte, HashSize)
	HashTo(v, key)
	return key
}

// HashTo calculates a hash of value v, storing it in a slice p.
func HashTo(v Value, p []byte) {
	h := hashPool.Get().(hash.Hash)
	h.Reset()
	defer hashPool.Put(h)
	if len(p) < HashSize {
		panic("buffer too small to fit the hash")
	}
	if v != nil {
		// TODO(kortschak,dennwc) Remove dependence on String() method.
		h.Write([]byte(v.String()))
	}
	h.Sum(p[:0])
}

// StringOf safely call v.String, returning empty string in case of nil Value.
func StringOf(v Value) string {
	if v == nil {
		return ""
	}
	return v.String()
}

// Raw is a Turtle/NQuads-encoded value.
type Raw string

func (s Raw) String() string { return string(s) }

// String is an RDF string value (ex: "name").
type String string

func (s String) String() string {
	//TODO(barakmich): Proper escaping.
	return `"` + string(s) + `"`
}

// TypedString is an RDF value with type (ex: "name"^^<type>).
type TypedString struct {
	Value String
	Type  IRI
}

func (s TypedString) String() string {
	return s.Value.String() + `^^` + s.Type.String()
}

// LangString is an RDF string with language (ex: "name"@lang).
type LangString struct {
	Value String
	Lang  string
}

func (s LangString) String() string {
	return s.Value.String() + `@` + s.Lang
}

// IRI is an RDF Internationalized Resource Identifier (ex: <name>).
type IRI string

func (s IRI) String() string { return `<` + string(s) + `>` }

// BNode is an RDF Blank Node (ex: _:name).
type BNode string

func (s BNode) String() string { return `_:` + string(s) }

// Native support for basic types
// TODO(dennwc): add method to TypedString to convert it to known native Value

// TODO(dennwc): make these configurable
const (
	defaultNamespace     = `http://schema.org/`
	defaultIntType   IRI = defaultNamespace + `Integer`
	defaultFloatType IRI = defaultNamespace + `Float`
	defaultBoolType  IRI = defaultNamespace + `Boolean`
	defaultTimeType  IRI = defaultNamespace + `DateTime`
)

// Int is a native wrapper for int64 type.
//
// It uses NQuad notation similar to TypedString.
type Int int64

func (s Int) String() string {
	return `"` + strconv.Itoa(int(s)) + `"^^<` + string(defaultIntType) + `>`
}

// Float is a native wrapper for float64 type.
//
// It uses NQuad notation similar to TypedString.
type Float float64

func (s Float) String() string {
	return `"` + strconv.FormatFloat(float64(s), 'E', -1, 64) + `"^^<` + string(defaultFloatType) + `>`
}

// Bool is a native wrapper for bool type.
//
// It uses NQuad notation similar to TypedString.
type Bool bool

func (s Bool) String() string {
	if bool(s) {
		return `"True"^^<` + string(defaultBoolType) + `>`
	}
	return `"False"^^<` + string(defaultBoolType) + `>`
}

var _ Equaler = Time{}

// Time is a native wrapper for time.Time type.
//
// It uses NQuad notation similar to TypedString.
type Time time.Time

func (s Time) String() string {
	// TODO(dennwc): this is used to compute hash, thus we might want to include nanos
	return `"` + time.Time(s).Format(time.RFC3339) + `"^^<` + string(defaultTimeType) + `>`
}
func (s Time) Equal(v Value) bool {
	t, ok := v.(Time)
	if !ok {
		return false
	}
	return time.Time(s).Equal(time.Time(t))
}
