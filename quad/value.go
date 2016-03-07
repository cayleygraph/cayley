package quad

import (
	"crypto/sha1"
	"hash"
	"sync"
)

type Value interface {
	String() string
}

const HashSize = sha1.Size

var hashPool = sync.Pool{
	New: func() interface{} { return sha1.New() },
}

// HashOf calculates a hash of value v
func HashOf(v Value) []byte {
	h := hashPool.Get().(hash.Hash)
	h.Reset()
	defer hashPool.Put(h)
	key := make([]byte, 0, HashSize)
	if v != nil {
		// TODO(kortschak,dennwc) Remove dependence on String() method.
		h.Write([]byte(v.String()))
	}
	key = h.Sum(key)
	return key
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
