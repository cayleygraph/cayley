package schema

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"golang.org/x/net/context"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/path"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/voc/rdf"
)

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

func toIRI(s string) quad.IRI {
	if s == "@type" {
		return quad.IRI(rdf.Type)
	}
	return quad.IRI(s)
}

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
	for _, s := range sub {
		if s == "opt" || s == "optional" {
			opt = true
		}
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
	if vs == "" || vs == any {
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

var Optimize bool

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

func iteratorForType(qs graph.QuadStore, root graph.Iterator, rt reflect.Type) (graph.Iterator, error) {
	p, err := PathForType(rt)
	if err != nil {
		return nil, err
	}
	return iteratorFromPath(qs, root, p)
}

var (
	pathForTypeMu sync.RWMutex
	pathForType   = make(map[reflect.Type]*path.Path)
)

func PathForType(rt reflect.Type) (*path.Path, error) {
	for rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct, got %v", rt)
	}

	pathForTypeMu.RLock()
	if p, ok := pathForType[rt]; ok {
		pathForTypeMu.RUnlock()
		return p, nil
	}
	pathForTypeMu.RUnlock()

	p := path.StartMorphism()
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if f.Anonymous { // TODO: handle anonymous fields
			return nil, fmt.Errorf("anonymous fields are not supported yet")
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
			p = p.Tag(name)
		case constraintRule:
			if rule.Rev {
				p = p.HasReverse(rule.Pred, rule.Val)
			} else {
				p = p.Has(rule.Pred, rule.Val)
			}
		case saveRule:
			if rule.Opt {
				if rule.Rev {
					p = p.SaveOptionalReverse(rule.Pred, name)
				} else {
					p = p.SaveOptional(rule.Pred, name)
				}
			} else {
				if rule.Rev {
					p = p.SaveReverse(rule.Pred, name)
				} else {
					p = p.Save(rule.Pred, name)
				}
			}
		}
	}
	pathForTypeMu.Lock()
	pathForType[rt] = p
	pathForTypeMu.Unlock()
	return p, nil
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
		return fmt.Errorf("cannot convert %v to %v", src.Type(), dst.Type())
	})
}

func saveToValue(ctx context.Context, qs graph.QuadStore, dst reflect.Value, m map[string][]graph.Value) error {
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
	for i := 0; i < rt.NumField(); i++ {
		select {
		case <-ctx.Done():
			return context.Canceled
		default:
		}
		f := rt.Field(i)
		name := f.Name
		rules := fields[name]
		if rules == nil {
			continue
		}
		if err := checkFieldType(f.Type); err != nil {
			return err
		}
		arr, ok := m[name]
		if !ok || len(arr) == 0 {
			continue
		}
		if f.Anonymous { // TODO(dennwc): handle anonymous fields
			return fmt.Errorf("anonymous fields (namely: %s) are not supported (yet)", f.Name)
		}
		ft := f.Type
		native := isNative(ft)
		for ft.Kind() == reflect.Ptr || ft.Kind() == reflect.Slice {
			native = native || isNative(ft)
			ft = ft.Elem()
		}
		for _, fv := range arr {
			var sv reflect.Value
			if !native && ft.Kind() == reflect.Struct {
				sv = reflect.New(ft).Elem()
				sit := qs.FixedIterator()
				sit.Add(fv)
				if err := SaveIteratorTo(ctx, qs, sv, sit); err != nil {
					return err
				}
			} else {
				fv := qs.NameOf(fv)
				if fv == nil {
					continue
				}
				sv = reflect.ValueOf(fv)
			}
			if err := DefaultConverter.SetValue(dst.Field(i), sv); err != nil {
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

// SaveTo
//
// Dst can be of kind Struct, Slice or Chan.
func SaveTo(ctx context.Context, qs graph.QuadStore, dst interface{}, ids ...quad.Value) error {
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
	return SaveIteratorTo(ctx, qs, rv, it)
}

// SaveIteratorTo
//
// Dst can be of kind Struct, Slice or Chan.
func SaveIteratorTo(ctx context.Context, qs graph.QuadStore, dst reflect.Value, list graph.Iterator) error {
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
	it, err := iteratorForType(qs, list, et)
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
		if err := saveToValue(ctx, qs, cur, mo); err != nil {
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
	return nil
}

func writeOneValReflect(w quadWriter, id quad.Value, pred quad.Value, rv reflect.Value, rev bool) error {
	if rv.Interface() == reflect.Zero(rv.Type()).Interface() { // TODO(dennwc): rewrite
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

func writeValueAs(w quadWriter, id quad.Value, rv reflect.Value, pref string, rules fieldRules) (quad.Value, error) {
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	rt := rv.Type()
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if f.Anonymous {
			if _, err := writeValueAs(w, id, rv.Field(i), pref+f.Name+".", rules); err != nil {
				return nil, err
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
				return nil, err
			}
		case saveRule:
			if f.Type.Kind() == reflect.Slice {
				sl := rv.Field(i)
				for j := 0; j < sl.Len(); j++ {
					if err := writeOneValReflect(w, id, r.Pred, sl.Index(j), r.Rev); err != nil {
						return nil, err
					}
				}
			} else {
				if err := writeOneValReflect(w, id, r.Pred, rv.Field(i), r.Rev); err != nil {
					return nil, err
				}
			}
		}
	}
	return id, nil
}

// quadWriter is an interface to write quads.
//
// TODO(dennwc): replace when the same interface will be exposed in graph/quads
type quadWriter interface {
	WriteQuad(quad.Quad) error
}

func idFor(rules fieldRules, rt reflect.Type, rv reflect.Value) (id quad.Value, err error) {
	for i := 0; i < rt.NumField(); i++ {
		fld := rt.Field(i)
		if _, ok := rules[fld.Name].(idRule); ok {
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
	return
}

// GenerateID gets called then each object without an ID field is saved.
var GenerateID func() quad.Value = func() quad.Value {
	return quad.NextBlankNode()
}

// WriteAsQuads writes a single value in form of quads into specified quad writer.
//
// It returns an identifier of the object in the output subgraph. If an object has
// an annotated ID field, it's value will be converted to quad.Value and returned.
// Otherwise, a new BNode will be generated.
func WriteAsQuads(w quadWriter, o interface{}) (quad.Value, error) {
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
		panic(fmt.Errorf("no rules for struct: %v", rt))
		return nil, fmt.Errorf("no rules for struct: %v", rt)
	}
	id, err := idFor(rules, rt, rv)
	if err != nil {
		return nil, err
	}
	if id == nil {
		id = GenerateID()
	}
	return writeValueAs(w, id, rv, "", rules)
}
