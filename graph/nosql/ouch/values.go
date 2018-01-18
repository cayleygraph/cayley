package ouch

import (
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/cayleygraph/cayley/graph/nosql"
)

const (
	keySeparator = "|"
)

// toOuchValue serializes nosql.Value -> native json values.
func toOuchValue(v nosql.Value) interface{} {
	switch v := v.(type) {
	case nil:
		return nil
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
		return time.Time(v).UTC().Format(time.RFC3339Nano)
	case nosql.Bytes:
		return base64.StdEncoding.EncodeToString(v)
	default:
		panic(fmt.Errorf("unsupported type: %T", v))
	}
}

func toOuchDoc(col, id, rev string, d nosql.Document) map[string]interface{} {
	if d == nil {
		return nil
	}
	m := make(map[string]interface{})
	if col != "" {
		m[collectionField] = col
	}
	if id != "" {
		m[idField] = id
	}
	if rev != "" {
		m[revField] = rev
	}

	for k, v := range d {
		if len(k) == 0 {
			continue
		}
		if sub, ok := v.(nosql.Document); ok {
			for sk, sv := range sub {
				path := k + keySeparator + sk
				m[path] = toOuchValue(sv)
			}
		} else {
			m[k] = toOuchValue(v)
		}
	}

	return m
}

func fromOuchValue(v interface{}) nosql.Value {
	switch v := v.(type) {
	case nil:
		return nil
	case []interface{}:
		out := make([]string, 0, len(v))
		for _, o := range v {
			s, ok := o.(string)
			if !ok {
				panic(fmt.Errorf("unexpected type in array: %T", o))
			}
			out = append(out, s)
		}
		return nosql.Strings(out)
	case string:
		return nosql.String(v)
	case float64:
		return nosql.Float(v)
	case bool:
		return nosql.Bool(v)
	default:
		panic(fmt.Errorf("unsupported type: %T", v))
	}
}

func fromOuchDoc(d map[string]interface{}) nosql.Document {
	if d == nil {
		return nil
	}
	m := make(nosql.Document, len(d))
	for k, v := range d {
		switch k {
		case "", idField, revField, collectionField:
			continue // don't pass these fields back to nosql
		}
		if k[0] != ' ' { // ignore any other ouch driver internal keys
			if path := strings.Split(k, keySeparator); len(path) > 1 {
				if len(path) != 2 {
					panic("nosql.Document nesting too deep")
				}
				// we have a sub-document
				if _, found := m[path[0]]; !found {
					m[path[0]] = make(nosql.Document)
				}
				m[path[0]].(nosql.Document)[path[1]] = fromOuchValue(v)
			} else {
				m[k] = fromOuchValue(v)
			}
		}
	}

	if len(m) == 0 {
		return nil
	}

	return m
}
