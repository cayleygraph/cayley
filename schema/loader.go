package schema

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/cayleygraph/cayley/graph/path"
	"github.com/cayleygraph/quad"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
)

var (
	errNotFound               = errors.New("not found")
	errRequiredFieldIsMissing = errors.New("required field is missing")
)

// Optimize flags controls an optimization step performed before queries.
var Optimize = true

// IsNotFound check if error is related to a missing object (either because of wrong ID or because of type constrains).
func IsNotFound(err error) bool {
	return err == errNotFound || err == errRequiredFieldIsMissing
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
func (c *Config) LoadTo(ctx context.Context, qs graph.QuadStore, dst interface{}, ids ...quad.Value) error {
	return c.LoadToDepth(ctx, qs, dst, -1, ids...)
}

// LoadToDepth is the same as LoadTo, but stops at a specified depth.
// Negative value means unlimited depth, and zero means top level only.
func (c *Config) LoadToDepth(ctx context.Context, qs graph.QuadStore, dst interface{}, depth int, ids ...quad.Value) error {
	if dst == nil {
		return fmt.Errorf("nil destination object")
	}
	var it graph.Iterator
	if len(ids) != 0 {
		fixed := iterator.NewFixed()
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
	return c.LoadIteratorToDepth(ctx, qs, rv, depth, it)
}

// LoadPathTo is the same as LoadTo, but starts loading objects from a given path.
func (c *Config) LoadPathTo(ctx context.Context, qs graph.QuadStore, dst interface{}, p *path.Path) error {
	return c.LoadIteratorTo(ctx, qs, reflect.ValueOf(dst), p.BuildIterator())
}

// LoadIteratorTo is a lower level version of LoadTo.
//
// It expects an iterator of nodes to be passed explicitly and
// destination value to be obtained via reflect package manually.
//
// Nodes iterator can be nil, All iterator will be used in this case.
func (c *Config) LoadIteratorTo(ctx context.Context, qs graph.QuadStore, dst reflect.Value, list graph.Iterator) error {
	return c.LoadIteratorToDepth(ctx, qs, dst, -1, list)
}

// LoadIteratorToDepth is the same as LoadIteratorTo, but stops at a specified depth.
// Negative value means unlimited depth, and zero means top level only.
func (c *Config) LoadIteratorToDepth(ctx context.Context, qs graph.QuadStore, dst reflect.Value, depth int, list graph.Iterator) error {
	if depth >= 0 {
		// 0 depth means "current level only" for user, but it's easier to make depth=0 a stop condition
		depth++
	}
	l := c.newLoader(qs)
	return l.loadIteratorToDepth(ctx, dst, depth, list)
}

type loader struct {
	c  *Config
	qs graph.QuadStore

	pathForType     map[reflect.Type]*path.Path
	pathForTypeRoot map[reflect.Type]*path.Path

	seen map[quad.Value]reflect.Value
}

func (c *Config) newLoader(qs graph.QuadStore) *loader {
	return &loader{
		c:  c,
		qs: qs,

		pathForType:     make(map[reflect.Type]*path.Path),
		pathForTypeRoot: make(map[reflect.Type]*path.Path),

		seen: make(map[quad.Value]reflect.Value),
	}
}

func (l *loader) makePathForType(rt reflect.Type, tagPref string, rootOnly bool) (*path.Path, error) {
	for rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct, got %v", rt)
	}
	if tagPref == "" {
		m := l.pathForType
		if rootOnly {
			m = l.pathForTypeRoot
		}
		if p, ok := m[rt]; ok {
			return p, nil
		}
	}

	p := path.StartMorphism()

	if iri := getTypeIRI(rt); iri != quad.IRI("") {
		p = p.Has(l.c.iri(iriType), iri)
	}

	// TODO(dennwc): rewrite to shapes

	allOptional := true
	var alt *path.Path
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if f.Anonymous {
			pa, err := l.makePathForType(f.Type, tagPref+f.Name+".", rootOnly)
			if err != nil {
				return nil, err
			}
			p = p.Follow(pa)
			continue
		}
		name := f.Name
		rule, err := l.c.fieldRule(f)
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
			allOptional = false
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
						if allOptional {
							ap := path.StartMorphism().HasReverse(rule.Pred)
							if alt == nil {
								alt = ap
							} else {
								alt = alt.Or(ap)
							}
						}
					} else {
						p = p.SaveOptional(rule.Pred, tag)
						if allOptional {
							ap := path.StartMorphism().Has(rule.Pred)
							if alt == nil {
								alt = ap
							} else {
								alt = alt.Or(ap)
							}
						}
					}
				}
			} else if rootOnly { // do not save field, enforce constraint only
				allOptional = false
				if rule.Rev {
					p = p.HasReverse(rule.Pred)
				} else {
					p = p.Has(rule.Pred)
				}
			} else {
				allOptional = false
				if rule.Rev {
					p = p.SaveReverse(rule.Pred, tag)
				} else {
					p = p.Save(rule.Pred, tag)
				}
			}
		}
	}
	if allOptional {
		p = p.And(alt.Unique())
	}
	if tagPref != "" {
		return p, nil
	}
	m := l.pathForType
	if rootOnly {
		m = l.pathForTypeRoot
	}
	m[rt] = p
	return p, nil
}

func (l *loader) loadToValue(ctx context.Context, dst reflect.Value, depth int, m map[string][]graph.Ref, tagPref string) error {
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
		nfields, err := l.c.rulesFor(rt)
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
			return ctx.Err()
		default:
		}
		f := rt.Field(i)
		name := f.Name
		if err := checkFieldType(f.Type); err != nil {
			return err
		}
		df := dst.Field(i)
		if f.Anonymous {
			if err := l.loadToValue(ctx, df, depth, m, tagPref+name+"."); err != nil {
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
		ptr := ft.Kind() == reflect.Ptr
		for ft.Kind() == reflect.Ptr || ft.Kind() == reflect.Slice {
			ft = ft.Elem()
			native = native || isNative(ft)
			switch ft.Kind() {
			case reflect.Ptr:
				ptr = true
			case reflect.Slice:
				ptr = false
			}
		}
		recursive := !native && ft.Kind() == reflect.Struct
		for _, fv := range arr {
			var sv reflect.Value
			if recursive {
				if ptr {
					fv := l.qs.NameOf(fv)
					var ok bool
					sv, ok = l.seen[fv]
					if ok && sv.Type().AssignableTo(f.Type) {
						df.Set(sv)
						continue
					}
				}
				sv = reflect.New(ft).Elem()
				err := l.loadIteratorToDepth(ctx, sv, depth-1, iterator.NewFixed(fv))
				if err == errRequiredFieldIsMissing {
					continue
				} else if err != nil {
					return err
				}
			} else {
				fv := l.qs.NameOf(fv)
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

func (l *loader) iteratorForType(root graph.Iterator, rt reflect.Type, rootOnly bool) (graph.Iterator, error) {
	p, err := l.makePathForType(rt, "", rootOnly)
	if err != nil {
		return nil, err
	}
	return l.iteratorFromPath(root, p)
}

func mergeMap(dst map[string][]graph.Ref, m map[string]graph.Ref) {
loop:
	for k, v := range m {
		sl := dst[k]
		for _, sv := range sl {
			if keysEqual(sv, v) {
				continue loop
			}
		}
		dst[k] = append(sl, v)
	}
}

func (l *loader) loadIteratorToDepth(ctx context.Context, dst reflect.Value, depth int, list graph.Iterator) error {
	if ctx == nil {
		ctx = context.TODO()
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
	fields, err := l.c.rulesFor(et)
	if err != nil {
		return err
	}

	ctxDone := func() bool {
		select {
		case <-ctx.Done():
			return true
		default:
		}
		return false
	}

	if ctxDone() {
		return ctx.Err()
	}

	rootOnly := depth == 0
	it, err := l.iteratorForType(list, et, rootOnly)
	if err != nil {
		return err
	}
	defer it.Close()

	ctx = context.WithValue(ctx, fieldsCtxKey{}, fields)
	for it.Next(ctx) {
		if ctxDone() {
			return ctx.Err()
		}
		id := l.qs.NameOf(it.Result())
		if id != nil {
			if sv, ok := l.seen[id]; ok {
				if slice {
					dst.Set(reflect.Append(dst, sv.Elem()))
				} else if chanl {
					dst.Send(sv.Elem())
				} else if dst.Kind() != reflect.Ptr {
					dst.Set(sv.Elem())
					return nil
				} else {
					dst.Set(sv)
					return nil
				}
				continue
			}
		}
		mp := make(map[string]graph.Ref)
		it.TagResults(mp)
		if len(mp) == 0 {
			continue
		}
		cur := dst
		if slice || chanl {
			cur = reflect.New(et)
		}
		mo := make(map[string][]graph.Ref, len(mp))
		for k, v := range mp {
			mo[k] = []graph.Ref{v}
		}
		for it.NextPath(ctx) {
			if ctxDone() {
				return ctx.Err()
			}
			mp = make(map[string]graph.Ref)
			it.TagResults(mp)
			if len(mp) == 0 {
				continue
			}
			// TODO(dennwc): replace with something more efficient
			mergeMap(mo, mp)
		}
		if id != nil {
			sv := cur
			if sv.Kind() != reflect.Ptr && sv.CanAddr() {
				sv = sv.Addr()
			}
			l.seen[id] = sv
		}
		err := l.loadToValue(ctx, cur, depth, mo, "")
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
	if list != nil { // TODO(dennwc): optional optimization: do this only if iterator is not "all nodes"
		// distinguish between missing object and type constraints
		list.Reset()
		and := iterator.NewAnd(list, l.qs.NodesAllIterator())
		defer and.Close()
		if and.Next(ctx) {
			return errRequiredFieldIsMissing
		}
	}
	return errNotFound
}

func (l *loader) iteratorFromPath(root graph.Iterator, p *path.Path) (graph.Iterator, error) {
	it := p.BuildIteratorOn(l.qs)
	if root != nil {
		it = iterator.NewAnd(root, it)
	}
	if Optimize {
		it, _ = it.Optimize()
	}
	return it, nil
}
