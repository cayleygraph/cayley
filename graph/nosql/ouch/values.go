package ouch

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cayleygraph/cayley/graph/nosql"
)

const (
	int64Adjust  = 1 << 63
	keySeparator = "|"
	timeFormat   = time.RFC3339Nano // seconds resolution only without Nano
)

// itos serializes int64 into a sortable string 13 chars long.
// NOTE: in JS there are no native 64bit integers.
func itos(i int64) string {
	s := strconv.FormatUint(uint64(i)+int64Adjust, 32)
	const z = "0000000000000"
	return z[len(s):] + s
}

// stoi de-serializes int64 from a sortable string 13 chars long.
func stoi(s string) int64 {
	ret, err := strconv.ParseUint(s, 32, 64)
	if err != nil {
		//TODO handle error?
		return 0
	}
	return int64(ret - int64Adjust)
}

// toOuchValue serializes nosql.Value -> native json values.
func toOuchValue(k string, v nosql.Value) interface{} {
	switch v := v.(type) {
	case nil:
		return nil
	case nosql.Strings: // special handling here, as type can't be inferred from json
		return "K" + strings.Join(v, keySeparator)
	case nosql.String:
		return "S" + string(v) // need leading "S"
	case nosql.Int: // special handling here, as type can't be inferred from json
		return "I" + itos(int64(v))
	case nosql.Float:
		return float64(v)
	case nosql.Bool:
		return bool(v)
	case nosql.Time: // special handling here, as type can't be inferred from json
		ret := "T" + time.Time(v).UTC().Format(timeFormat)
		return ret
	case nosql.Bytes: // special handling here, as type can't be inferred from json
		return "B" + base64.StdEncoding.EncodeToString(v)
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
		if subDoc, found := v.(nosql.Document); found {
			for subK, subV := range subDoc {
				subPath := k + keySeparator + subK
				m[subPath] = toOuchValue(subPath, subV)
			}
		} else {
			m[k] = toOuchValue(k, v)
		}
	}

	return m
}

func fromOuchValue(k string, v interface{}) nosql.Value {
	switch v := v.(type) {
	case nil:
		return nil
	case string:
		if len(v) == 0 {
			return nil
		}
		typ := v[0]
		v = v[1:]
		switch typ {
		case 'S':
			return nosql.String(v)

		case 'K':
			parts := strings.Split(v, keySeparator)
			key := make(nosql.Key, 0, len(parts))
			for _, part := range parts {
				key = append(key, part)
			}
			return key.Value()

		case 'B':
			byts, err := base64.StdEncoding.DecodeString(v)
			if err != nil {
				// TODO consider how to handle this error properly
				return nosql.Bytes(nil)
			}
			return nosql.Bytes(byts)

		case 'T':
			var time0 nosql.Time
			tim, err := time.Parse(timeFormat, v)
			if err != nil {
				// TODO consider how to handle this error properly
				fmt.Println("DEBUG Time parse", tim, err)
				return time0
			}
			return nosql.Time(tim)

		case 'I':
			return nosql.Int(stoi(v))

		default:
			panic(fmt.Errorf("unsupported serialized type: %v%v", typ, v))
		}
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
				m[path[0]].(nosql.Document)[path[1]] = fromOuchValue(k, v)
			} else {
				m[k] = fromOuchValue(k, v)
			}
		}
	}

	if len(m) == 0 {
		return nil
	}

	return m
}
