package linkedql

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/cayleygraph/quad"
)

var (
	TypeByName = make(map[string]reflect.Type)
	nameByType = make(map[reflect.Type]string)
)

// Register adds an Item type to the registry.
func Register(typ RegistryItem) {
	tp := reflect.TypeOf(typ)
	if tp.Kind() == reflect.Ptr {
		tp = tp.Elem()
	}
	if tp.Kind() != reflect.Struct {
		panic("only structs are allowed")
	}
	name := string(typ.Type())
	if _, ok := TypeByName[name]; ok {
		panic("this name was already registered")
	}
	TypeByName[name] = tp
	nameByType[tp] = name
}

var quadValue = reflect.TypeOf((*quad.Value)(nil)).Elem()
var quadSliceValue = reflect.TypeOf([]quad.Value{})
var quadIRI = reflect.TypeOf(quad.IRI(""))
var quadSliceIRI = reflect.TypeOf([]quad.IRI{})

// Unmarshal attempts to unmarshal an Item or returns error.
func Unmarshal(data []byte) (RegistryItem, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	var typ string
	if err := json.Unmarshal(m["@type"], &typ); err != nil {
		return nil, err
	}
	delete(m, "@type")
	tp, ok := TypeByName[typ]
	if !ok {
		return nil, fmt.Errorf("unsupported item: %q", typ)
	}
	item := reflect.New(tp).Elem()
	for i := 0; i < tp.NumField(); i++ {
		f := tp.Field(i)
		name := f.Name
		tag := strings.SplitN(f.Tag.Get("json"), ",", 2)[0]
		if tag == "-" {
			continue
		} else if tag != "" {
			name = tag
		}
		v, ok := m[name]
		if !ok {
			continue
		}
		fv := item.Field(i)
		switch f.Type {
		case quadValue:
			var a interface{}
			err := json.Unmarshal(v, &a)
			if err != nil {
				return nil, err
			}
			value, err := parseValue(v)
			if err != nil {
				return nil, err
			}
			fv.Set(reflect.ValueOf(value))
			continue
		case quadSliceValue:
			var a []interface{}
			err := json.Unmarshal(v, &a)
			if err != nil {
				return nil, err
			}
			var values []quad.Value
			for _, item := range a {
				value, err := parseValue(item)
				if err != nil {
					return nil, err
				}
				values = append(values, value)
			}
			fv.Set(reflect.ValueOf(values))
			continue
		case quadIRI:
			var a interface{}
			err := json.Unmarshal(v, &a)
			if err != nil {
				return nil, err
			}
			value, err := parseIRI(v)
			if err != nil {
				return nil, err
			}
			fv.Set(reflect.ValueOf(*value))
			continue
		case quadSliceIRI:
			var a []interface{}
			err := json.Unmarshal(v, &a)
			if err != nil {
				return nil, err
			}
			var values []quad.IRI
			for _, item := range a {
				value, err := parseIRI(item)
				if err != nil {
					return nil, err
				}
				values = append(values, *value)
			}
			fv.Set(reflect.ValueOf(values))
			continue
		}
		switch f.Type.Kind() {
		case reflect.Interface:
			s, err := Unmarshal(v)
			if err != nil {
				return nil, err
			}
			fv.Set(reflect.ValueOf(s))
		case reflect.Slice:
			el := f.Type.Elem()
			if el.Kind() != reflect.Interface {
				err := json.Unmarshal(v, fv.Addr().Interface())
				if err != nil {
					return nil, err
				}
			} else {
				var arr []json.RawMessage
				if err := json.Unmarshal(v, &arr); err != nil {
					return nil, err
				}
				if arr != nil {
					va := reflect.MakeSlice(f.Type, len(arr), len(arr))
					for i, v := range arr {
						s, err := Unmarshal(v)
						if err != nil {
							return nil, err
						}
						va.Index(i).Set(reflect.ValueOf(s))
					}
					fv.Set(va)
				}
			}
		default:
			err := json.Unmarshal(v, fv.Addr().Interface())
			if err != nil {
				return nil, err
			}
		}
	}
	return item.Addr().Interface().(RegistryItem), nil
}

func parseBNode(a interface{}) (*quad.BNode, error) {
	pointer, err := parseIdentifierString(a)
	id := *pointer
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(id, "_:") {
		return nil, fmt.Errorf("blank node ID must start with \"_:\"")
	}
	bnode := quad.BNode(id[2:])
	return &bnode, nil
}

func parseIRI(a interface{}) (*quad.IRI, error) {
	pointer, err := parseIdentifierString(a)
	id := *pointer
	if err != nil {
		return nil, err
	}
	iri := quad.IRI(id)
	return &iri, nil
}

func parseIdentifierString(a interface{}) (*string, error) {
	m, ok := a.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Not a map")
	}
	id, ok := m["@id"].(string)
	if !ok {
		return nil, fmt.Errorf("No string @id key")
	}
	return &id, nil
}

func parseLiteral(a interface{}) (quad.Value, error) {
	switch a := a.(type) {
	case string:
		return quad.String(a), nil
	case int64:
		return quad.Int(a), nil
	case float64:
		return quad.Float(a), nil
	case bool:
		return quad.Bool(a), nil
	case map[string]interface{}:
		value, ok := a["@value"].(string)
		if ok {
			if language, ok := a["@language"].(string); ok {
				return quad.LangString{Value: quad.String("value"), Lang: language}, nil
			}
			if _type, ok := a["@type"].(string); ok {
				return quad.TypedString{Value: quad.String(value), Type: quad.IRI(_type)}, nil
			}
		}
	}
	return nil, fmt.Errorf("Can not parse %#v as literal", a)
}

func parseValue(a interface{}) (quad.Value, error) {
	bnode, err := parseBNode(a)
	if err == nil {
		return bnode, nil
	}
	iri, err := parseIRI(a)
	if err == nil {
		return iri, nil
	}
	literal, err := parseLiteral(a)
	if err == nil {
		return literal, nil
	}
	return nil, fmt.Errorf("Can not parse JSON-LD value: %#v", a)
}

// RegistryItem in the registry.
type RegistryItem interface {
	Type() quad.IRI
}
