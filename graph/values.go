package graph

import (
	"encoding/hex"
	"fmt"

	"github.com/cayleygraph/quad"
)

// Ref defines an opaque "quad store reference" type. However the backend wishes
// to implement it, a Ref is merely a token to a quad or a node that the
// backing store itself understands, and the base iterators pass around.
//
// For example, in a very traditional, graphd-style graph, these are int64s
// (guids of the primitives). In a very direct sort of graph, these could be
// pointers to structs, or merely quads, or whatever works best for the
// backing store.
//
// These must be comparable, or return a comparable version on Key.
type Ref interface {
	// Key returns a dynamic type that is comparable according to the Go language specification.
	// The returned value must be unique for each receiver value.
	Key() interface{}
}

// Value is an alias for Ref.
//
// Deprecated: use Ref instead.
type Value = Ref

func HashOf(s quad.Value) (out ValueHash) {
	if s == nil {
		return
	}
	quad.HashTo(s, out[:])
	return
}

var _ Ref = ValueHash{}

// ValueHash is a hash of a single value.
type ValueHash [quad.HashSize]byte

func (h ValueHash) Valid() bool {
	return h != ValueHash{}
}
func (h ValueHash) Key() interface{} { return h }
func (h ValueHash) String() string {
	if !h.Valid() {
		return ""
	}
	return hex.EncodeToString(h[:])
}

// PreFetchedValue is an optional interface for graph.Ref to indicate that
// quadstore has already loaded a value into memory.
type PreFetchedValue interface {
	Ref
	NameOf() quad.Value
}

func PreFetched(v quad.Value) PreFetchedValue {
	return fetchedValue{v}
}

type fetchedValue struct {
	Val quad.Value
}

func (v fetchedValue) IsNode() bool       { return true }
func (v fetchedValue) NameOf() quad.Value { return v.Val }
func (v fetchedValue) Key() interface{}   { return v.Val }

// Keyer provides a method for comparing types that are not otherwise comparable.
// The Key method must return a dynamic type that is comparable according to the
// Go language specification. The returned value must be unique for each receiver
// value.
//
// Deprecated: Ref contains the same method now.
type Keyer = Ref

// ToKey prepares Ref to be stored inside maps, calling Key() if necessary.
func ToKey(v Ref) interface{} {
	if v == nil {
		return nil
	}
	return v.Key()
}

var _ Ref = QuadHash{}

type QuadHash struct {
	Subject   ValueHash
	Predicate ValueHash
	Object    ValueHash
	Label     ValueHash
}

func (q QuadHash) Dirs() [4]ValueHash {
	return [4]ValueHash{
		q.Subject,
		q.Predicate,
		q.Object,
		q.Label,
	}
}
func (q QuadHash) Key() interface{} { return q }
func (q QuadHash) Get(d quad.Direction) ValueHash {
	switch d {
	case quad.Subject:
		return q.Subject
	case quad.Predicate:
		return q.Predicate
	case quad.Object:
		return q.Object
	case quad.Label:
		return q.Label
	}
	panic(fmt.Errorf("unknown direction: %v", d))
}
func (q *QuadHash) Set(d quad.Direction, h ValueHash) {
	switch d {
	case quad.Subject:
		q.Subject = h
	case quad.Predicate:
		q.Predicate = h
	case quad.Object:
		q.Object = h
	case quad.Label:
		q.Label = h
	default:
		panic(fmt.Errorf("unknown direction: %v", d))
	}
}
