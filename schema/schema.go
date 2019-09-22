// Package schema contains helpers to map Go objects to quads and vise-versa.
//
// This package is not a full schema library. It will not save or force any
// RDF schema constrains, it only provides a mapping.
package schema

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/path"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc/rdf"
)

var reflQuadValue = reflect.TypeOf((*quad.Value)(nil)).Elem()

type ErrReqFieldNotSet struct {
	Field string
}

func (e ErrReqFieldNotSet) Error() string {
	return fmt.Sprintf("required field is not set: %s", e.Field)
}

// IRIMode controls how IRIs are processed.
type IRIMode int

const (
	// IRINative applies no transformation to IRIs.
	IRINative = IRIMode(iota)
	// IRIShort will compact all IRIs with known namespaces.
	IRIShort
	// IRIFull will expand all IRIs with known namespaces.
	IRIFull
)

// NewConfig creates a new schema config.
func NewConfig() *Config {
	return &Config{
		IRIs: IRINative,
	}
}

// Config controls behavior of schema package.
type Config struct {
	// IRIs set a conversion mode for all IRIs.
	IRIs IRIMode

	// GenerateID is called when any object without an ID field is being saved.
	GenerateID func(_ interface{}) quad.Value

	// Label will be added to all quads written. Does not affect queries.
	Label quad.Value

	rulesForTypeMu sync.RWMutex
	rulesForType   map[reflect.Type]fieldRules
}

func (c *Config) genID(o interface{}) quad.Value {
	gen := c.GenerateID
	if gen == nil {
		gen = GenerateID
	}
	if gen == nil {
		gen = func(_ interface{}) quad.Value {
			return quad.RandomBlankNode()
		}
	}
	return gen(o)
}

type rule interface {
	isRule()
}

type constraintRule struct {
	Pred quad.IRI
	Val  quad.IRI
	Rev  bool
}

func (constraintRule) isRule() {}

type saveRule struct {
	Pred quad.IRI
	Rev  bool
	Opt  bool
}

func (saveRule) isRule() {}

type idRule struct{}

func (idRule) isRule() {}

const iriType = quad.IRI(rdf.Type)

func (c *Config) iri(v quad.IRI) quad.IRI {
	switch c.IRIs {
	case IRIShort:
		v = v.Short()
	case IRIFull:
		v = v.Full()
	}
	return v
}

func (c *Config) toIRI(s string) quad.IRI {
	var v quad.IRI
	if s == "@type" {
		v = iriType
	} else {
		v = quad.IRI(s)
	}
	return c.iri(v)
}

var reflEmptyStruct = reflect.TypeOf(struct{}{})

func (c Config) fieldRule(fld reflect.StructField) (rule, error) {
	tag := fld.Tag.Get("quad")
	sub := strings.Split(tag, ",")
	tag, sub = sub[0], sub[1:]
	const (
		trim      = ` `
		spo, ops  = `>`, `<`
		any, none = `*`, `-`
		this      = `@id`
	)
	tag = strings.Trim(tag, trim)
	jsn := false
	if tag == "" {
		tag = strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
		jsn = true
	}
	if tag == "" || tag == none {
		return nil, nil // ignore
	}
	rule := strings.Trim(tag, trim)
	if rule == this {
		return idRule{}, nil
	}
	opt := false
	req := false
	for _, s := range sub {
		if s == "opt" || s == "optional" {
			opt = true
		}
		if s == "req" || s == "required" {
			req = true
		}
	}
	if req {
		opt = false
	} else if fld.Type.Kind() == reflect.Slice {
		opt = true
	}

	rev := strings.Contains(rule, ops)
	var tri []string
	if jsn {
		tri = []string{rule}
	} else if rev { // o<p-s
		tri = strings.SplitN(rule, ops, 3)
		if len(tri) != 2 {
			return nil, fmt.Errorf("wrong quad tag format: '%s'", rule)
		}
	} else { // s-p>o // default
		tri = strings.SplitN(rule, spo, 3)
		if len(tri) > 2 { //len(tri) != 2 {
			return nil, fmt.Errorf("wrong quad tag format: '%s'", rule)
		}
	}
	var ps, vs string
	if rev {
		ps, vs = strings.Trim(tri[0], trim), strings.Trim(tri[1], trim)
	} else {
		ps, vs = strings.Trim(tri[0], trim), any
		if len(tri) > 1 {
			vs = strings.Trim(tri[1], trim)
		}
	}
	if ps == "" {
		return nil, fmt.Errorf("wrong quad format: '%s': no predicate", rule)
	}
	p := c.toIRI(ps)
	if vs == "" || vs == any && fld.Type != reflEmptyStruct {
		return saveRule{Pred: p, Rev: rev, Opt: opt}, nil
	} else {
		return constraintRule{Pred: p, Val: c.toIRI(vs), Rev: rev}, nil
	}
}

func checkFieldType(ftp reflect.Type) error {
	for ftp.Kind() == reflect.Ptr || ftp.Kind() == reflect.Slice {
		ftp = ftp.Elem()
	}
	switch ftp.Kind() {
	case reflect.Array: // TODO: support arrays
		return fmt.Errorf("array fields are not supported yet")
	case reflect.Func, reflect.Invalid:
		return fmt.Errorf("%v fields are not supported", ftp.Kind())
	default:
	}
	return nil
}

var (
	typesMu   sync.RWMutex
	typeToIRI = make(map[reflect.Type]quad.IRI)
	iriToType = make(map[quad.IRI]reflect.Type)
)

func getTypeIRI(rt reflect.Type) quad.IRI {
	typesMu.RLock()
	iri := typeToIRI[rt]
	typesMu.RUnlock()
	return iri
}

// RegisterType associates an IRI with a given Go type.
//
// All queries and writes will require or add a type triple.
func RegisterType(iri quad.IRI, obj interface{}) {
	var rt reflect.Type
	if obj != nil {
		if t, ok := obj.(reflect.Type); ok {
			rt = t
		} else {
			rt = reflect.TypeOf(obj)
			if rt.Kind() == reflect.Ptr {
				rt = rt.Elem()
			}
		}
	}
	full := iri.Full()
	typesMu.Lock()
	defer typesMu.Unlock()
	if obj == nil {
		tp := iriToType[full]
		delete(typeToIRI, tp)
		delete(iriToType, full)
		return
	}
	if _, exists := typeToIRI[rt]; exists {
		panic(fmt.Errorf("type %v is already registered", rt))
	}
	if _, exists := iriToType[full]; exists {
		panic(fmt.Errorf("IRI %v is already registered", iri))
	}
	typeToIRI[rt] = iri
	iriToType[full] = rt
}

// PathForType builds a path (morphism) for a given Go type.
func (c *Config) PathForType(rt reflect.Type) (*path.Path, error) {
	l := c.newLoader(nil)
	return l.makePathForType(rt, "", false)
}

func anonFieldType(fld reflect.StructField) (reflect.Type, bool) {
	ft := fld.Type
	if ft.Kind() == reflect.Ptr {
		ft = ft.Elem()
	}
	if ft.Kind() == reflect.Struct {
		return ft, true
	}
	return ft, false
}

func (c *Config) rulesForStructTo(out fieldRules, pref string, rt reflect.Type) error {
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		name := f.Name
		if f.Anonymous {
			if ft, ok := anonFieldType(f); !ok {
				return fmt.Errorf("anonymous fields of type %v are not supported", ft)
			} else if err := c.rulesForStructTo(out, pref+name+".", ft); err != nil {
				return err
			}
			continue
		}
		rules, err := c.fieldRule(f)
		if err != nil {
			return err
		}
		if rules != nil {
			out[pref+name] = rules
		}
	}
	return nil
}

// rulesFor
//
// Returned map should not be changed.
func (c *Config) rulesFor(rt reflect.Type) (fieldRules, error) {
	//	if rt.Kind() != reflect.Struct {
	//		return nil, fmt.Errorf("expected struct, got: %v", rt)
	//	}
	c.rulesForTypeMu.RLock()
	rules, ok := c.rulesForType[rt]
	c.rulesForTypeMu.RUnlock()
	if ok {
		return rules, nil
	}
	out := make(fieldRules)
	if err := c.rulesForStructTo(out, "", rt); err != nil {
		return nil, err
	}
	c.rulesForTypeMu.Lock()
	if c.rulesForType == nil {
		c.rulesForType = make(map[reflect.Type]fieldRules)
	}
	c.rulesForType[rt] = out
	c.rulesForTypeMu.Unlock()
	return out, nil
}

type fieldsCtxKey struct{}
type fieldRules map[string]rule

type ValueConverter interface {
	SetValue(dst reflect.Value, src reflect.Value) error
}

type ValueConverterFunc func(dst reflect.Value, src reflect.Value) error

func (f ValueConverterFunc) SetValue(dst reflect.Value, src reflect.Value) error { return f(dst, src) }

var DefaultConverter ValueConverter

type ErrTypeConversionFailed struct {
	From reflect.Type
	To   reflect.Type
}

func (e ErrTypeConversionFailed) Error() string {
	return fmt.Sprintf("cannot convert %v to %v", e.From, e.To)
}

func init() {
	DefaultConverter = ValueConverterFunc(func(dst reflect.Value, src reflect.Value) error {
		dt, st := dst.Type(), src.Type()
		if dt == st || (dt.Kind() == reflect.Interface && st.Implements(dt)) {
			dst.Set(src)
			return nil
		} else if st.ConvertibleTo(dt) {
			dst.Set(src.Convert(dt))
			return nil
		} else if dt.Kind() == reflect.Ptr {
			v := reflect.New(dt.Elem())
			if err := DefaultConverter.SetValue(v.Elem(), src); err != nil {
				return err
			}
			dst.Set(v)
			return nil
		} else if dt.Kind() == reflect.Slice {
			v := reflect.New(dt.Elem())
			if err := DefaultConverter.SetValue(v.Elem(), src); err != nil {
				return err
			}
			dst.Set(reflect.Append(dst, v.Elem()))
			return nil
		}
		return ErrTypeConversionFailed{From: src.Type(), To: dst.Type()}
	})
}

func isNative(rt reflect.Type) bool { // TODO(dennwc): replace
	_, ok := quad.AsValue(reflect.Zero(rt).Interface())
	return ok
}

func keysEqual(v1, v2 graph.Ref) bool {
	type key interface {
		Key() interface{}
	}
	e1, ok1 := v1.(key)
	e2, ok2 := v2.(key)
	if ok1 != ok2 {
		return false
	}
	if ok1 && ok2 {
		return e1.Key() == e2.Key()
	}
	return v1 == v2
}

func isExported(name string) bool {
	ch, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(ch)
}

func isZero(rv reflect.Value) bool {
	switch rv.Kind() {
	case reflect.Ptr:
		return rv.IsNil()
	case reflect.Slice, reflect.Map:
		return rv.IsNil() || rv.Len() == 0
	case reflect.Struct:
		// have to be careful here - struct may contain slice fields,
		// so we cannot compare them directly
		rt := rv.Type()
		exported := 0
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			if !isExported(f.Name) {
				continue
			}
			exported++
			if !isZero(rv.Field(i)) {
				return false
			}
		}
		if exported != 0 {
			return true
		}
		// opaque type - compare directly
	}
	// primitive types
	return rv.Interface() == reflect.Zero(rv.Type()).Interface()
}

func (c *Config) idFor(rules fieldRules, rt reflect.Type, rv reflect.Value, pref string) (id quad.Value, err error) {
	hasAnon := false
	for i := 0; i < rt.NumField(); i++ {
		fld := rt.Field(i)
		hasAnon = hasAnon || fld.Anonymous
		if _, ok := rules[pref+fld.Name].(idRule); ok {
			vid := rv.Field(i).Interface()
			switch vid := vid.(type) {
			case quad.IRI:
				id = c.iri(vid)
			case quad.BNode:
				id = vid
			case string:
				id = c.toIRI(vid)
			default:
				err = fmt.Errorf("unsupported type for id field: %T", vid)
			}
			return
		}
	}
	if !hasAnon {
		return
	}
	// second pass - look for anonymous fields
	for i := 0; i < rt.NumField(); i++ {
		fld := rt.Field(i)
		if !fld.Anonymous {
			continue
		}
		id, err = c.idFor(rules, fld.Type, rv.Field(i), pref+fld.Name+".")
		if err != nil || id != nil {
			return
		}
	}
	return
}
