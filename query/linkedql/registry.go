package linkedql

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/cayleygraph/quad"
)

var (
	typeByName = make(map[string]reflect.Type)
	nameByType = make(map[reflect.Type]string)
)

func Register(typ Step) {
	tp := reflect.TypeOf(typ)
	if tp.Kind() == reflect.Ptr {
		tp = tp.Elem()
	}
	if tp.Kind() != reflect.Struct {
		panic("only structs are allowed")
	}
	name := string(typ.Type())
	if _, ok := typeByName[name]; ok {
		panic("this name was already registered")
	}
	typeByName[name] = tp
	nameByType[tp] = name
}

var quadValue = reflect.TypeOf((*quad.Value)(nil)).Elem()
var quadSliceValue = reflect.TypeOf(([]quad.Value)(nil))

func UnmarshalStep(data []byte) (Step, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	var typ string
	if err := json.Unmarshal(m["@type"], &typ); err != nil {
		return nil, err
	}
	delete(m, "@type")
	tp, ok := typeByName[typ]
	if !ok {
		return nil, fmt.Errorf("unsupported step: %q", typ)
	}
	step := reflect.New(tp).Elem()
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
		fv := step.Field(i)
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
		}
		switch f.Type.Kind() {
		case reflect.Interface:
			s, err := UnmarshalStep(v)
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
						s, err := UnmarshalStep(v)
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
	return step.Addr().Interface().(Step), nil
}

func parseValue(a interface{}) (quad.Value, error) {
	switch a := a.(type) {
	case string:
		return quad.String(a), nil
	case map[string]interface{}:
		id, ok := a["@id"].(string)
		if ok {
			if strings.HasPrefix(id, "_:") {
				return quad.BNode(id[2:]), nil
			}
			return quad.IRI(id), nil
		}
		_, ok = a["@value"].(string)
		if ok {
			panic("Doesn't support special literals yet")
		}
	}
	return nil, errors.New("Couldn't parse rawValue to a quad.Value")
}
