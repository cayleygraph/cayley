package schema

import (
	"fmt"
	"reflect"

	"github.com/cayleygraph/quad"
)

// WriteAsQuads writes a single value in form of quads into specified quad writer.
//
// It returns an identifier of the object in the output sub-graph. If an object has
// an annotated ID field, it's value will be converted to quad.Value and returned.
// Otherwise, a new BNode will be generated using GenerateID function.
//
// See LoadTo for a list of quads mapping rules.
func (c *Config) WriteAsQuads(w quad.Writer, o interface{}) (quad.Value, error) {
	wr := c.newWriter(w)
	return wr.writeAsQuads(reflect.ValueOf(o))
}

type writer struct {
	c    *Config
	w    quad.Writer
	seen map[uintptr]quad.Value
}

func (c *Config) newWriter(w quad.Writer) *writer {
	return &writer{c: c, w: w, seen: make(map[uintptr]quad.Value)}
}

func (w *writer) writeQuad(s, p, o quad.Value, rev bool) error {
	if rev {
		s, o = o, s
	}
	return w.w.WriteQuad(quad.Quad{Subject: s, Predicate: p, Object: o, Label: w.c.Label})
}

// writeOneValReflect writes a set of quads corresponding to a value. It may omit writing quads if value is zero.
func (w *writer) writeOneValReflect(id quad.Value, pred quad.Value, rv reflect.Value, rev bool) error {
	if isZero(rv) {
		return nil
	}
	// write field value and get an ID
	sid, err := w.writeAsQuads(rv)
	if err != nil {
		return err
	}
	// write a quad pointing to this value
	return w.writeQuad(id, pred, sid, rev)
}

func (w *writer) writeTypeInfo(id quad.Value, rt reflect.Type) error {
	iri := getTypeIRI(rt)
	if iri == quad.IRI("") {
		return nil
	}
	return w.writeQuad(id, w.c.iri(iriType), w.c.iri(iri), false)
}

func (w *writer) writeValueAs(id quad.Value, rv reflect.Value, pref string, rules fieldRules) error {
	switch kind := rv.Kind(); kind {
	case reflect.Ptr, reflect.Map:
		ptr := rv.Pointer()
		if _, ok := w.seen[ptr]; ok {
			return nil
		}
		w.seen[ptr] = id
		if kind == reflect.Ptr {
			rv = rv.Elem()
		}
	}
	rt := rv.Type()
	if err := w.writeTypeInfo(id, rt); err != nil {
		return err
	}
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if f.Anonymous {
			if err := w.writeValueAs(id, rv.Field(i), pref+f.Name+".", rules); err != nil {
				return err
			}
			continue
		}
		switch r := rules[pref+f.Name].(type) {
		case constraintRule:
			s, o := id, quad.Value(r.Val)
			if r.Rev {
				s, o = o, s
			}
			if err := w.writeQuad(s, r.Pred, o, false); err != nil {
				return err
			}
		case saveRule:
			if f.Type.Kind() == reflect.Slice {
				sl := rv.Field(i)
				for j := 0; j < sl.Len(); j++ {
					if err := w.writeOneValReflect(id, r.Pred, sl.Index(j), r.Rev); err != nil {
						return err
					}
				}
			} else {
				fv := rv.Field(i)
				if !r.Opt && isZero(fv) {
					return ErrReqFieldNotSet{Field: f.Name}
				}
				if err := w.writeOneValReflect(id, r.Pred, fv, r.Rev); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (w *writer) writeAsQuads(rv reflect.Value) (quad.Value, error) {
	rt := rv.Type()
	// if node is a primitive - return directly
	if rt.Implements(reflQuadValue) {
		return rv.Interface().(quad.Value), nil
	}
	prv := rv
	kind := rt.Kind()
	// check if we've seen this node already
	switch kind {
	case reflect.Ptr, reflect.Map:
		ptr := prv.Pointer()
		if sid, ok := w.seen[ptr]; ok {
			return sid, nil
		}
		if kind == reflect.Ptr {
			rv = rv.Elem()
			rt = rv.Type()
			kind = rt.Kind()
		}
	}
	// check if it's a type that quads package supports
	// note, that it may be a struct such as time.Time
	if val, ok := quad.AsValue(rv.Interface()); ok {
		return val, nil
	} else if kind == reflect.String {
		return quad.String(rv.String()), nil
	} else if kind == reflect.Int || kind == reflect.Uint ||
		kind == reflect.Int32 || kind == reflect.Uint32 ||
		kind == reflect.Int16 || kind == reflect.Uint16 ||
		kind == reflect.Int8 || kind == reflect.Uint8 {
		return quad.Int(rv.Int()), nil
	} else if kind == reflect.Float64 || kind == reflect.Float32 {
		return quad.Float(rv.Float()), nil
	} else if kind == reflect.Bool {
		return quad.Bool(rv.Bool()), nil
	}
	// TODO(dennwc): support maps
	if kind != reflect.Struct {
		return nil, fmt.Errorf("unsupported type: %v", rt)
	}
	// get conversion rules for this struct type
	rules, err := w.c.rulesFor(rt)
	if err != nil {
		return nil, fmt.Errorf("can't load rules: %v", err)
	}
	if len(rules) == 0 {
		return nil, fmt.Errorf("no rules for struct: %v", rt)
	}
	// get an ID from the struct value
	id, err := w.c.idFor(rules, rt, rv, "")
	if err != nil {
		return nil, err
	}
	if id == nil {
		id = w.c.genID(prv.Interface())
	}
	// save a node ID to avoid loops
	switch prv.Kind() {
	case reflect.Ptr, reflect.Map:
		ptr := prv.Pointer()
		w.seen[ptr] = id
	}
	if err = w.writeValueAs(id, rv, "", rules); err != nil {
		return nil, err
	}
	return id, nil
}
