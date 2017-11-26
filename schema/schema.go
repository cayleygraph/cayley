// Package schema contains helpers to map Go objects to quads and vise-versa.
//
// This package is not a full schema library. It will not save or force any
// RDF schema constrains, it only provides a mapping.
package schema

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/path"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/voc"
	"github.com/cayleygraph/cayley/voc/rdf"
)

type ErrReqFieldNotSet struct {
	Field string
}

func (e ErrReqFieldNotSet) Error() string {
	return fmt.Sprintf("required field is not set: %s", e.Field)
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

func toIRI(s string) quad.IRI {
	if s == "@type" {
		return iriType
	}
	return quad.IRI(s)
}

var reflEmptyStruct = reflect.TypeOf(struct{}{})

func fieldRule(fld reflect.StructField) (rule, error) {
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
	p := toIRI(ps)
	if vs == "" || vs == any && fld.Type != reflEmptyStruct {
		return saveRule{Pred: p, Rev: rev, Opt: opt}, nil
	} else {
		return constraintRule{Pred: p, Val: toIRI(vs), Rev: rev}, nil
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

// Optimize flags controls an optimization step performed before queries.
var Optimize = true

func iteratorFromPath(qs graph.QuadStore, root graph.Iterator, p *path.Path) (graph.Iterator, error) {
	it := p.BuildIteratorOn(qs)
	if root != nil {
		it = iterator.NewAnd(qs, root, it)
	}
	if Optimize {
		it, _ = it.Optimize()
		it, _ = qs.OptimizeIterator(it)
	}
	return it, nil
}

func iteratorForType(qs graph.QuadStore, root graph.Iterator, rt reflect.Type, rootOnly bool) (graph.Iterator, error) {
	p, err := makePathForType(rt, "", rootOnly)
	if err != nil {
		return nil, err
	}
	return iteratorFromPath(qs, root, p)
}

var (
	typesMu   sync.RWMutex
	typeToIRI = make(map[reflect.Type]quad.IRI)
	iriToType = make(map[quad.IRI]reflect.Type)

	pathForTypeMu   sync.RWMutex
	pathForType     = make(map[reflect.Type]*path.Path)
	pathForTypeRoot = make(map[reflect.Type]*path.Path)
)

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

func makePathForType(rt reflect.Type, tagPref string, rootOnly bool) (*path.Path, error) {
	for rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct, got %v", rt)
	}
	if tagPref != "" {
		pathForTypeMu.RLock()
		m := pathForType
		if rootOnly {
			m = pathForTypeRoot
		}
		p, ok := m[rt]
		pathForTypeMu.RUnlock()
		if ok {
			return p, nil
		}
	}

	p := path.StartMorphism()
	typesMu.RLock()
	iri := typeToIRI[rt]
	typesMu.RUnlock()
	if iri != quad.IRI("") {
		p = p.Has(iriType, iri)
	}
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if f.Anonymous {
			pa, err := makePathForType(f.Type, tagPref+f.Name+".", rootOnly)
			if err != nil {
				return nil, err
			}
			p = p.Follow(pa)
			continue
		}
		name := f.Name
		rule, err := fieldRule(f)
		if err != nil {
			return nil, err
		} else if rule == nil { // skip
			continue
		}
		ft := f.Type
		if ft.Kind() == reflect.Ptr {
			ft = ft.Elem()
		}
		if err = checkFieldType(ft); err != nil {
			return nil, err
		}
		switch rule := rule.(type) {
		case idRule:
			p = p.Tag(tagPref + name)
		case constraintRule:
			var nodes []quad.Value
			if rule.Val != "" {
				nodes = []quad.Value{rule.Val}
			}
			if rule.Rev {
				p = p.HasReverse(rule.Pred, nodes...)
			} else {
				p = p.Has(rule.Pred, nodes...)
			}
		case saveRule:
			tag := tagPref + name
			if rule.Opt {
				if !rootOnly {
					if rule.Rev {
						p = p.SaveOptionalReverse(rule.Pred, tag)
					} else {
						p = p.SaveOptional(rule.Pred, tag)
					}
				}
			} else if rootOnly { // do not save field, enforce constraint only
				if rule.Rev {
					p = p.HasReverse(rule.Pred)
				} else {
					p = p.Has(rule.Pred)
				}
			} else {
				if rule.Rev {
					p = p.SaveReverse(rule.Pred, tag)
				} else {
					p = p.Save(rule.Pred, tag)
				}
			}
		}
	}
	if tagPref != "" {
		pathForTypeMu.Lock()
		m := pathForType
		if rootOnly {
			m = pathForTypeRoot
		}
		m[rt] = p
		pathForTypeMu.Unlock()
	}
	return p, nil
}

// PathForType builds a path (morphism) for a given Go type.
func PathForType(rt reflect.Type) (*path.Path, error) {
	return makePathForType(rt, "", false)
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

func rulesForStructTo(out fieldRules, pref string, rt reflect.Type) error {
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		name := f.Name
		if f.Anonymous {
			if ft, ok := anonFieldType(f); !ok {
				return fmt.Errorf("anonymous fields of type %v are not supported", ft)
			} else if err := rulesForStructTo(out, pref+name+".", ft); err != nil {
				return err
			}
			continue
		}
		rules, err := fieldRule(f)
		if err != nil {
			return err
		}
		if rules != nil {
			out[pref+name] = rules
		}
	}
	return nil
}

var (
	rulesForType   = make(map[reflect.Type]fieldRules)
	rulesForTypeMu sync.RWMutex
)

// RulesFor
//
// Returned map should not be changed.
func rulesFor(rt reflect.Type) (fieldRules, error) {
	//	if rt.Kind() != reflect.Struct {
	//		return nil, fmt.Errorf("expected struct, got: %v", rt)
	//	}
	rulesForTypeMu.RLock()
	if rules, ok := rulesForType[rt]; ok {
		rulesForTypeMu.RUnlock()
		return rules, nil
	}
	rulesForTypeMu.RUnlock()
	out := make(fieldRules)
	if err := rulesForStructTo(out, "", rt); err != nil {
		return nil, err
	}
	rulesForTypeMu.Lock()
	rulesForType[rt] = out
	rulesForTypeMu.Unlock()
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

// IsNotFound check if error is related to a missing object (either because of wrong ID or because of type constrains).
func IsNotFound(err error) bool {
	return err == errNotFound || err == errRequiredFieldIsMissing
}

var (
	errNotFound               = errors.New("not found")
	errRequiredFieldIsMissing = errors.New("required field is missing")
)

func loadToValue(ctx context.Context, qs graph.QuadStore, dst reflect.Value, depth int, m map[string][]graph.Value, tagPref string) error {
	if ctx == nil {
		ctx = context.TODO()
	}
	for dst.Kind() == reflect.Ptr {
		dst = dst.Elem()
	}
	rt := dst.Type()
	if rt.Kind() != reflect.Struct {
		return fmt.Errorf("expected struct, got %v", rt)
	}
	var fields fieldRules
	if v := ctx.Value(fieldsCtxKey{}); v != nil {
		fields = v.(fieldRules)
	} else {
		nfields, err := rulesFor(rt)
		if err != nil {
			return err
		}
		fields = nfields
	}
	if depth != 0 { // do not check required fields if depth limit is reached
		for name, field := range fields {
			if r, ok := field.(saveRule); ok && !r.Opt {
				if vals := m[name]; len(vals) == 0 {
					return errRequiredFieldIsMissing
				}
			}
		}
	}
	for i := 0; i < rt.NumField(); i++ {
		select {
		case <-ctx.Done():
			return context.Canceled
		default:
		}
		f := rt.Field(i)
		name := f.Name
		if err := checkFieldType(f.Type); err != nil {
			return err
		}
		df := dst.Field(i)
		if f.Anonymous {
			if err := loadToValue(ctx, qs, df, depth, m, tagPref+name+"."); err != nil {
				return fmt.Errorf("load anonymous field %s failed: %v", f.Name, err)
			}
			continue
		}
		rules := fields[tagPref+name]
		if rules == nil {
			continue
		}
		arr, ok := m[tagPref+name]
		if !ok || len(arr) == 0 {
			continue
		}
		ft := f.Type
		native := isNative(ft)
		for ft.Kind() == reflect.Ptr || ft.Kind() == reflect.Slice {
			native = native || isNative(ft)
			ft = ft.Elem()
		}
		recursive := !native && ft.Kind() == reflect.Struct
		for _, fv := range arr {
			var sv reflect.Value
			if recursive {
				sv = reflect.New(ft).Elem()
				sit := qs.FixedIterator()
				sit.Add(fv)
				err := loadIteratorToDepth(ctx, qs, sv, depth-1, sit)
				if err == errRequiredFieldIsMissing {
					continue
				} else if err != nil {
					return err
				}
			} else {
				fv := qs.NameOf(fv)
				if fv == nil {
					continue
				}
				sv = reflect.ValueOf(fv)
			}
			if err := DefaultConverter.SetValue(df, sv); err != nil {
				return fmt.Errorf("field %s: %v", f.Name, err)
			}
		}
	}
	return nil
}

func isNative(rt reflect.Type) bool { // TODO(dennwc): replace
	_, ok := quad.AsValue(reflect.Zero(rt).Interface())
	return ok
}

func keysEqual(v1, v2 graph.Value) bool {
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

// LoadTo will load a sub-graph of objects starting from ids (or from any nodes, if empty)
// to a destination Go object. Destination can be a struct, slice or channel.
//
// Mapping to quads is done via Go struct tag "quad" or "json" as a fallback.
//
// A simplest mapping is an "@id" tag which saves node ID (subject of a quad) into tagged field.
//
//	type Node struct{
//		ID quad.IRI `json:"@id"` // or `quad:"@id"`
// 	}
//
// Field with an "@id" tag is omitted, but in case of Go->quads mapping new ID will be generated
// using GenerateID callback, which can be changed to provide a custom mappings.
//
// All other tags are interpreted as a predicate name for a specific field:
//
//	type Person struct{
//		ID quad.IRI `json:"@id"`
//		Name string `json:"name"`
// 	}
//	p := Person{"bob","Bob"}
//	// is equivalent to triple:
//	// <bob> <name> "Bob"
//
// Predicate IRIs in RDF can have a long namespaces, but they can be written in short
// form. They will be expanded automatically if namespace prefix is registered within
// QuadStore or globally via "voc" package.
// There is also a special predicate name "@type" which is mapped to "rdf:type" IRI.
//
//	voc.RegisterPrefix("ex:", "http://example.org/")
//	type Person struct{
//		ID quad.IRI `json:"@id"`
//		Type quad.IRI `json:"@type"`
//		Name string `json:"ex:name"` // will be expanded to http://example.org/name
// 	}
//	p := Person{"bob",quad.IRI("Person"),"Bob"}
//	// is equivalent to triples:
//	// <bob> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <Person>
//	// <bob> <http://example.org/name> "Bob"
//
// Predicate link direction can be reversed with a special tag syntax (not available for "json" tag):
//
// 	type Person struct{
//		ID quad.IRI `json:"@id"`
//		Name string `json:"name"` // same as `quad:"name"` or `quad:"name > *"`
//		Parents []quad.IRI `quad:"isParentOf < *"`
// 	}
//	p := Person{"bob","Bob",[]quad.IRI{"alice","fred"}}
//	// is equivalent to triples:
//	// <bob> <name> "Bob"
//	// <alice> <isParentOf> <bob>
//	// <fred> <isParentOf> <bob>
//
// All fields in structs are interpreted as required (except slices), thus struct will not be
// loaded if one of fields is missing. An "optional" tag can be specified to relax this requirement.
// Also, "required" can be specified for slices to alter default value.
//
//	type Person struct{
//		ID quad.IRI `json:"@id"`
//		Name string `json:"name"` // required field
//		ThirdName string `quad:"thirdName,optional"` // can be empty
//		FollowedBy []quad.IRI `quad:"follows"`
// 	}
func LoadTo(ctx context.Context, qs graph.QuadStore, dst interface{}, ids ...quad.Value) error {
	return LoadToDepth(ctx, qs, dst, -1, ids...)
}

// LoadToDepth is the same as LoadTo, but stops at a specified depth.
// Negative value means unlimited depth, and zero means top level only.
func LoadToDepth(ctx context.Context, qs graph.QuadStore, dst interface{}, depth int, ids ...quad.Value) error {
	if dst == nil {
		return fmt.Errorf("nil destination object")
	}
	var it graph.Iterator
	if len(ids) != 0 {
		fixed := qs.FixedIterator()
		for _, id := range ids {
			fixed.Add(qs.ValueOf(id))
		}
		it = fixed
	}
	var rv reflect.Value
	if v, ok := dst.(reflect.Value); ok {
		rv = v
	} else {
		rv = reflect.ValueOf(dst)
	}
	return LoadIteratorToDepth(ctx, qs, rv, depth, it)
}

// LoadPathTo is the same as LoadTo, but starts loading objects from a given path.
func LoadPathTo(ctx context.Context, qs graph.QuadStore, dst interface{}, p *path.Path) error {
	return LoadIteratorTo(ctx, qs, reflect.ValueOf(dst), p.BuildIterator())
}

// LoadIteratorTo is a lower level version of LoadTo.
//
// It expects an iterator of nodes to be passed explicitly and
// destination value to be obtained via reflect package manually.
//
// Nodes iterator can be nil, All iterator will be used in this case.
func LoadIteratorTo(ctx context.Context, qs graph.QuadStore, dst reflect.Value, list graph.Iterator) error {
	return LoadIteratorToDepth(ctx, qs, dst, -1, list)
}

// LoadIteratorToDepth is the same as LoadIteratorTo, but stops at a specified depth.
// Negative value means unlimited depth, and zero means top level only.
func LoadIteratorToDepth(ctx context.Context, qs graph.QuadStore, dst reflect.Value, depth int, list graph.Iterator) error {
	if depth >= 0 {
		// 0 depth means "current level only" for user, but it's easier to make depth=0 a stop condition
		depth++
	}
	return loadIteratorToDepth(ctx, qs, dst, depth, list)
}

func loadIteratorToDepth(ctx context.Context, qs graph.QuadStore, dst reflect.Value, depth int, list graph.Iterator) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if dst.Kind() == reflect.Ptr {
		dst = dst.Elem()
	}
	et := dst.Type()
	slice, chanl := false, false
	if dst.Kind() == reflect.Slice {
		et = et.Elem()
		slice = true
	} else if dst.Kind() == reflect.Chan {
		et = et.Elem()
		chanl = true
		defer dst.Close()
	}
	fields, err := rulesFor(et)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	rootOnly := depth == 0
	it, err := iteratorForType(qs, list, et, rootOnly)
	if err != nil {
		return err
	}
	defer it.Close()

	ctx = context.WithValue(ctx, fieldsCtxKey{}, fields)
	for it.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		mp := make(map[string]graph.Value)
		it.TagResults(mp)
		if len(mp) == 0 {
			continue
		}
		cur := dst
		if slice || chanl {
			cur = reflect.New(et)
		}
		mo := make(map[string][]graph.Value, len(mp))
		for k, v := range mp {
			mo[k] = []graph.Value{v}
		}
		for it.NextPath() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			mp = make(map[string]graph.Value)
			it.TagResults(mp)
			if len(mp) == 0 {
				continue
			}
			// TODO(dennwc): replace with more efficient
			for k, v := range mp {
				if sl, ok := mo[k]; !ok {
					mo[k] = []graph.Value{v}
				} else if len(sl) == 1 {
					if !keysEqual(sl[0], v) {
						mo[k] = append(sl, v)
					}
				} else {
					found := false
					for _, sv := range sl {
						if keysEqual(sv, v) {
							found = true
							break
						}
					}
					if !found {
						mo[k] = append(sl, v)
					}
				}
			}
		}
		err := loadToValue(ctx, qs, cur, depth, mo, "")
		if err == errRequiredFieldIsMissing {
			if !slice && !chanl {
				return err
			}
			continue
		} else if err != nil {
			return err
		}
		if slice {
			dst.Set(reflect.Append(dst, cur.Elem()))
		} else if chanl {
			dst.Send(cur.Elem())
		} else {
			return nil
		}
	}
	if err := it.Err(); err != nil {
		return err
	}
	if slice || chanl {
		return nil
	}
	if list != nil && list.Type() != graph.All {
		// distinguish between missing object and type constraints
		list.Reset()
		and := iterator.NewAnd(qs, list, qs.NodesAllIterator())
		defer and.Close()
		if and.Next() {
			return errRequiredFieldIsMissing
		}
	}
	return errNotFound
}

func isZero(rv reflect.Value) bool {
	return rv.Interface() == reflect.Zero(rv.Type()).Interface() // TODO(dennwc): rewrite
}

func writeOneValReflect(w quad.Writer, id quad.Value, pred quad.Value, rv reflect.Value, rev bool) error {
	if isZero(rv) {
		return nil
	}
	targ, ok := quad.AsValue(rv.Interface())
	if !ok {
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}
		targ, ok = quad.AsValue(rv.Interface())
		if !ok && rv.Kind() == reflect.Struct {
			sid, err := WriteAsQuads(w, rv.Interface())
			if err != nil {
				return err
			}
			targ, ok = sid, true
		}
	}
	if !ok {
		return fmt.Errorf("unsupported type: %T", rv.Interface())
	}
	s, o := id, targ
	if rev {
		s, o = o, s
	}
	return w.WriteQuad(quad.Quad{s, pred, o, nil})
}

func writeValueAs(w quad.Writer, id quad.Value, rv reflect.Value, pref string, rules fieldRules) error {
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	rt := rv.Type()
	typesMu.RLock()
	iri := typeToIRI[rt]
	typesMu.RUnlock()
	if iri != quad.IRI("") {
		if err := w.WriteQuad(quad.Quad{id, iriType, iri, nil}); err != nil {
			return err
		}
	}
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if f.Anonymous {
			if err := writeValueAs(w, id, rv.Field(i), pref+f.Name+".", rules); err != nil {
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
			if err := w.WriteQuad(quad.Quad{s, r.Pred, o, nil}); err != nil {
				return err
			}
		case saveRule:
			if f.Type.Kind() == reflect.Slice {
				sl := rv.Field(i)
				for j := 0; j < sl.Len(); j++ {
					if err := writeOneValReflect(w, id, r.Pred, sl.Index(j), r.Rev); err != nil {
						return err
					}
				}
			} else {
				fv := rv.Field(i)
				if !r.Opt && isZero(fv) {
					return ErrReqFieldNotSet{Field: f.Name}
				}
				if err := writeOneValReflect(w, id, r.Pred, fv, r.Rev); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func idFor(rules fieldRules, rt reflect.Type, rv reflect.Value, pref string) (id quad.Value, err error) {
	hasAnon := false
	for i := 0; i < rt.NumField(); i++ {
		fld := rt.Field(i)
		hasAnon = hasAnon || fld.Anonymous
		if _, ok := rules[pref+fld.Name].(idRule); ok {
			vid := rv.Field(i).Interface()
			switch vid := vid.(type) {
			case quad.IRI:
				id = vid
			case quad.BNode:
				id = vid
			case string:
				id = quad.IRI(vid)
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
		id, err = idFor(rules, fld.Type, rv.Field(i), fld.Name+".")
		if err != nil || id != nil {
			return
		}
	}
	return
}

// GenerateID is called when any object without an ID field is being saved.
var GenerateID func(interface{}) quad.Value = func(_ interface{}) quad.Value {
	return quad.RandomBlankNode()
}

// WriteAsQuads writes a single value in form of quads into specified quad writer.
//
// It returns an identifier of the object in the output sub-graph. If an object has
// an annotated ID field, it's value will be converted to quad.Value and returned.
// Otherwise, a new BNode will be generated using GenerateID function.
//
// See LoadTo for a list of quads mapping rules.
func WriteAsQuads(w quad.Writer, o interface{}) (quad.Value, error) {
	if v, ok := o.(quad.Value); ok {
		return v, nil
	}
	rv := reflect.ValueOf(o)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	rt := rv.Type()
	rules, err := rulesFor(rt)
	if err != nil {
		return nil, fmt.Errorf("can't load rules: %v", err)
	}
	if len(rules) == 0 {
		return nil, fmt.Errorf("no rules for struct: %v", rt)
	}
	id, err := idFor(rules, rt, rv, "")
	if err != nil {
		return nil, err
	}
	if id == nil {
		id = GenerateID(o)
	}
	if err = writeValueAs(w, id, rv, "", rules); err != nil {
		return nil, err
	}
	return id, nil
}

type namespace struct {
	_      struct{} `quad:"@type > cayley:namespace"`
	Full   quad.IRI `quad:"@id"`
	Prefix quad.IRI `quad:"cayley:prefix"`
}

// WriteNamespaces will writes namespaces list into graph.
func WriteNamespaces(w quad.Writer, n *voc.Namespaces) error {
	rules, err := rulesFor(reflect.TypeOf(namespace{}))
	if err != nil {
		return fmt.Errorf("can't load rules: %v", err)
	}
	for _, ns := range n.List() {
		obj := namespace{
			Full:   quad.IRI(ns.Full),
			Prefix: quad.IRI(ns.Prefix),
		}
		rv := reflect.ValueOf(obj)
		if err = writeValueAs(w, obj.Full, rv, "", rules); err != nil {
			return err
		}
	}
	return nil
}

// LoadNamespaces will load namespaces stored in graph to a specified list.
// If destination list is empty, global namespace registry will be used.
func LoadNamespaces(ctx context.Context, qs graph.QuadStore, dest *voc.Namespaces) error {
	var list []namespace
	if err := LoadTo(ctx, qs, &list); err != nil {
		return err
	}
	register := dest.Register
	if dest == nil {
		register = voc.Register
	}
	for _, ns := range list {
		register(voc.Namespace{
			Prefix: string(ns.Prefix),
			Full:   string(ns.Full),
		})
	}
	return nil
}
