package nosql

import (
	"bytes"
	"strings"
	"time"
)

// Value is a interface that limits a set of types that nosql database can handle.
type Value interface {
	isValue()
}

// Document is a type of item stored in nosql database.
type Document map[string]Value

func (Document) isValue() {}

// String is an UTF8 string value.
type String string

func (String) isValue() {}

// Int is an int value.
//
// Some databases might not distinguish Int value from Float.
// In this case implementation will take care of converting it to a correct type.
type Int int64

func (Int) isValue() {}

// Float is an floating point value.
//
// Some databases might not distinguish Int value from Float.
// In this case the package will take care of converting it to a correct type.
type Float float64

func (Float) isValue() {}

// Bool is a boolean value.
type Bool bool

func (Bool) isValue() {}

// Time is a timestamp value.
//
// Some databases has no type to represent time values.
// In this case string/json representation can be used and package will take care of converting it.
type Time time.Time

func (Time) isValue() {}

// Bytes is a raw binary data.
//
// Some databases has no type to represent binary data.
// In this case base64 representation can be used and package will take care of converting it.
type Bytes []byte

func (Bytes) isValue() {}

// Strings is an array of strings. Used mostly to store Keys.
type Strings []string

func (Strings) isValue() {}

// ValuesEqual returns true if values are strictly equal.
func ValuesEqual(v1, v2 Value) bool {
	switch v1 := v2.(type) {
	case Document:
		v2, ok := v2.(Document)
		if !ok || len(v1) != len(v2) {
			return false
		}
		for k, s1 := range v1 {
			if s2, ok := v2[k]; !ok || !ValuesEqual(s1, s2) {
				return false
			}
		}
		return true
	case Strings:
		v2, ok := v2.(Strings)
		if !ok || len(v1) != len(v2) {
			return false
		}
		for i := range v1 {
			if v1[i] != v2[i] {
				return false
			}
		}
		return true
	case Bytes:
		v2, ok := v2.(Bytes)
		if !ok || len(v1) != len(v2) {
			return false
		}
		return bytes.Equal(v1, v2)
	case Time:
		v2, ok := v2.(Time)
		if !ok {
			return false
		}
		return time.Time(v1).Equal(time.Time(v2))
	}
	return v1 == v2
}

// CompareValues return 0 if values are equal, positive value if first value sorts after second, and negative otherwise.
func CompareValues(v1, v2 Value) int {
	switch v1 := v1.(type) {
	case Document:
		v2, ok := v2.(Document)
		if !ok {
			return -1
		} else if len(v1) != len(v2) {
			return len(v1) - len(v2)
		}
		return -1 // TODO: implement proper sorting?
	case Strings:
		v2, ok := v2.(Strings)
		if !ok {
			return -1
		} else if len(v1) != len(v2) {
			return len(v1) - len(v2)
		}
		for i := range v1 {
			if dn := CompareValues(String(v1[i]), String(v2[i])); dn != 0 {
				return dn
			}
		}
		return 0
	case Bytes:
		v2, ok := v2.(Bytes)
		if !ok {
			return -1
		}
		return bytes.Compare(v1, v2)
	case Time:
		v2, ok := v2.(Time)
		if !ok {
			return -1
		}
		t1, t2 := time.Time(v1), time.Time(v2)
		if t1.Equal(t2) {
			return 0
		} else if t1.Before(t2) {
			return -1
		}
		return +1
	case String:
		v2, ok := v2.(String)
		if !ok {
			return -1
		}
		return strings.Compare(string(v1), string(v2))
	case Int:
		v2, ok := v2.(Int)
		if !ok {
			return -1
		}
		if v1 == v2 {
			return 0
		} else if v1 < v2 {
			return -1
		}
		return +1
	case Float:
		v2, ok := v2.(Float)
		if !ok {
			return -1
		}
		if v1 == v2 {
			return 0
		} else if v1 < v2 {
			return -1
		}
		return +1
	}
	return -1
}
