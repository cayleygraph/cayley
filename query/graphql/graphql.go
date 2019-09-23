package graphql

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"unicode"

	"github.com/dennwc/graphql/language/ast"
	"github.com/dennwc/graphql/language/lexer"
	"github.com/dennwc/graphql/language/parser"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/path"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/quad"
)

const Name = "graphql"

// GraphQL charset: [_A-Za-z][_0-9A-Za-z]*
// (https://facebook.github.io/graphql/#sec-Names)

// IRI charset: [^#x00-#x20<>"{}|^`\]
// (https://www.w3.org/TR/turtle/#grammar-production-IRIREF)

func allowedNameRune(r rune) bool {
	// will include <> in the IRI value
	return r > 0x20 && !strings.ContainsRune("\"{}()|^`", r) && !unicode.IsSpace(r)
}

func init() {
	lexer.AllowNameRunes = allowedNameRune

	query.RegisterLanguage(query.Language{
		Name: Name,
		Session: func(qs graph.QuadStore) query.Session {
			return NewSession(qs)
		},
		REPL: func(qs graph.QuadStore) query.REPLSession {
			return NewSession(qs)
		},
		HTTPError: httpError,
		HTTPQuery: httpQuery,
	})
}

func NewSession(qs graph.QuadStore) *Session {
	return &Session{qs: qs}
}

type Session struct {
	qs graph.QuadStore
}

func (s *Session) Execute(ctx context.Context, qu string, opt query.Options) (query.Iterator, error) {
	switch opt.Collation {
	case query.Raw, query.JSON, query.REPL:
	default:
		return nil, &query.ErrUnsupportedCollation{Collation: opt.Collation}
	}
	q, err := Parse(strings.NewReader(qu))
	if err != nil {
		return nil, err
	}
	return &results{
		s:   s,
		q:   q,
		col: opt.Collation,
	}, nil
}

type results struct {
	s   *Session
	q   *Query
	col query.Collation
	res map[string]interface{}
	err error
}

func (it *results) Next(ctx context.Context) bool {
	if it.q == nil {
		return false
	}
	it.res, it.err = it.q.Execute(ctx, it.s.qs)
	it.q = nil
	return it.err == nil && len(it.res) != 0
}

func (it *results) Result() interface{} {
	if len(it.res) == 0 {
		return nil
	}
	if it.col != query.REPL {
		return it.res
	}
	data, _ := json.MarshalIndent(it.res, "", "   ")
	return string(data)
}

func (it *results) Err() error {
	return it.err
}

func (it *results) Close() error {
	it.q = nil
	return nil
}

// Configurable keywords and special field names.
var (
	ValueKey = "id"
	LimitKey = "first"
	SkipKey  = "offset"
	AnyKey   = "*"
)

type Query struct {
	fields []field
}

type has struct {
	Via    quad.IRI
	Rev    bool
	Values []quad.Value
	Labels []quad.Value
}

type field struct {
	Via       quad.IRI
	Alias     string
	Rev       bool
	Opt       bool
	Labels    []quad.Value
	Has       []has
	Fields    []field
	AllFields bool // fetch all fields
	UnNest    bool // all fields will be saved to parent object
}

func (f field) isSave() bool { return len(f.Has)+len(f.Fields) == 0 && !f.AllFields }

type object struct {
	id     graph.Ref
	fields map[string]interface{}
}

func buildIterator(qs graph.QuadStore, p *path.Path) graph.Iterator {
	it, _ := p.BuildIterator().Optimize()
	return it
}

func iterateObject(ctx context.Context, qs graph.QuadStore, f *field, p *path.Path) (out []map[string]interface{}, _ error) {
	if len(f.Labels) != 0 {
		p = p.LabelContext(f.Labels)
	} else {
		p = p.LabelContext()
	}
	var (
		limit = -1
		skip  = 0
	)

	for _, h := range f.Has {
		switch h.Via {
		case quad.IRI(ValueKey): // special key - "id"
			p = p.Is(h.Values...)
		case quad.IRI(LimitKey), quad.IRI(SkipKey): // limit and skip
			if len(h.Values) != 1 {
				return nil, fmt.Errorf("unexpected arguments: %v (%d)", h.Values, len(h.Values))
			}
			n, ok := h.Values[0].(quad.Int)
			if !ok {
				return nil, fmt.Errorf("unexpected value type for %v: %T", string(h.Via), h.Values[0])
			}
			if h.Via == quad.IRI(LimitKey) {
				limit = int(n)
			} else {
				skip = int(n)
				if skip < 0 {
					skip = 0
				}
			}
		default: // everything else - Has constraint
			if len(h.Labels) != 0 {
				p = p.LabelContext(h.Labels)
			}
			if h.Rev {
				p = p.HasReverse(h.Via, h.Values...)
			} else {
				p = p.Has(h.Via, h.Values...)
			}
			if len(h.Labels) != 0 {
				p = p.LabelContext()
			}
		}
	}
	tail := func() {
		if skip > 0 {
			p = p.Skip(int64(skip))
		}
		if limit >= 0 {
			p = p.Limit(int64(limit))
		}
	}
	if f.AllFields {
		tail()

		it := buildIterator(qs, p)
		defer it.Close()

		// we don't care about alternative paths to nodes here, so we will not call NextPath
		// and we haven't tagged anything, so we will not call TagResult either
		for i := 0; limit < 0 || i < limit; i++ {
			select {
			case <-ctx.Done():
				return out, ctx.Err()
			default:
			}
			if !it.Next(ctx) {
				break
			}
			nv := it.Result()
			obj := make(map[string]interface{})
			obj[ValueKey] = qs.NameOf(nv)
			func() {
				sit := qs.QuadIterator(quad.Subject, nv)
				defer sit.Close()
				for sit.Next(ctx) {
					q := qs.Quad(sit.Result())
					if p, ok := q.Predicate.(quad.IRI); ok {
						obj[string(p)] = q.Object
					} else {
						obj[quad.ToString(q.Predicate)] = q.Object
					}
				}
			}()
			out = append(out, obj)
		}
		return out, it.Err()
	}
	unnest := make(map[string]bool)
	for _, f2 := range f.Fields {
		if f2.UnNest {
			unnest[f2.Alias] = true
		}
		if !f2.isSave() {
			continue
		}
		if f2.Via == quad.IRI(ValueKey) {
			p = p.Tag(f2.Alias)
			continue
		}
		if len(f2.Labels) != 0 {
			p = p.LabelContext(f2.Labels)
		}
		if f2.Opt {
			if f2.Rev {
				p = p.SaveOptionalReverse(f2.Via, f2.Alias)
			} else {
				p = p.SaveOptional(f2.Via, f2.Alias)
			}
		} else {
			if f2.Rev {
				p = p.SaveReverse(f2.Via, f2.Alias)
			} else {
				p = p.Save(f2.Via, f2.Alias)
			}
		}
		if len(f2.Labels) != 0 {
			p = p.LabelContext()
		}
	}
	tail()

	// first, collect result node ids and any tags associated with it (flat values)
	it := buildIterator(qs, p)
	defer it.Close()

	var results []object
	for i := 0; limit < 0 || i < limit; i++ {
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		default:
		}
		if !it.Next(ctx) {
			break
		}
		fields := make(map[string][]graph.Ref)

		tags := make(map[string]graph.Ref)
		it.TagResults(tags)
		for k, v := range tags {
			fields[k] = []graph.Ref{v}
		}
		for it.NextPath(ctx) {
			select {
			case <-ctx.Done():
				return out, ctx.Err()
			default:
			}
			tags = make(map[string]graph.Ref)
			it.TagResults(tags)
		dedup:
			for k, v := range tags {
				vals := fields[k]
				for _, v2 := range vals {
					if graph.ToKey(v) == graph.ToKey(v2) {
						continue dedup
					}
				}
				fields[k] = append(vals, v)
			}
		}
		obj := object{id: it.Result()}
		if len(fields) > 0 {
			obj.fields = make(map[string]interface{}, len(fields))
			for k, arr := range fields {
				vals, err := graph.ValuesOf(ctx, qs, arr)
				if err != nil {
					return nil, err
				}
				if len(vals) == 1 {
					obj.fields[k] = vals[0]
				} else {
					obj.fields[k] = vals
				}
			}
		}
		results = append(results, obj)
	}
	if err := it.Err(); err != nil {
		return out, err
	}

	// next, load complex objects inside fields
	for _, r := range results {
		obj := r.fields
		if obj == nil {
			obj = make(map[string]interface{})
		}
		for _, f2 := range f.Fields {
			if f2.isSave() {
				continue // skip flat values
			}
			// start from saved id for a field node
			p2 := path.StartPathNodes(qs, r.id)
			if len(f2.Labels) != 0 {
				p2 = p2.LabelContext(f2.Labels)
			}
			if f2.Rev {
				p2 = p2.In(f2.Via)
			} else {
				p2 = p2.Out(f2.Via)
			}
			if len(f2.Labels) != 0 {
				p2 = p2.LabelContext()
			}
			arr, err := iterateObject(ctx, qs, &f2, p2)
			if err != nil {
				return out, err
			}
			if f2.UnNest {
				if len(arr) > 1 {
					return nil, fmt.Errorf("cannot unnest more than one object on %q; use (%s: 1) to force",
						f2.Alias, LimitKey)
				} else if len(arr) == 0 {
					continue
				}
				for k, v := range arr[0] {
					obj[k] = v
				}
			} else {
				var v interface{}
				if len(arr) == 1 {
					v = arr[0]
				} else if len(arr) > 1 {
					v = arr
				}
				obj[f2.Alias] = v
			}
		}
		out = append(out, obj)
	}
	return out, nil
}

func (q *Query) Execute(ctx context.Context, qs graph.QuadStore) (map[string]interface{}, error) {
	out := make(map[string]interface{})
	for _, f := range q.fields {
		arr, err := iterateObject(ctx, qs, &f, path.StartPath(qs))
		if err != nil {
			return out, err
		}
		var v interface{}
		if len(arr) == 1 {
			v = arr[0]
		} else if len(arr) > 1 {
			v = arr
		}
		out[f.Alias] = v
	}
	return out, nil
}

func Parse(r io.Reader) (*Query, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	doc, err := parser.Parse(parser.ParseParams{Source: string(data)})
	if err != nil {
		return nil, err
	}
	if len(doc.Definitions) != 1 {
		return nil, fmt.Errorf("unsupported query type")
	}
	def, ok := doc.Definitions[0].(*ast.OperationDefinition)
	if !ok {
		return nil, fmt.Errorf("unsupported query type: %T", doc.Definitions[0])
	} else if def.Operation != "query" {
		return nil, fmt.Errorf("unsupported operation: %s", def.Operation)
	}
	fields, all, err := setToFields(def.SelectionSet, nil)
	if err != nil {
		return nil, err
	} else if all {
		return nil, fmt.Errorf("expand all is not supported at top level")
	}
	return &Query{fields: fields}, nil
}

func setToFields(set *ast.SelectionSet, labels []quad.Value) (out []field, all bool, _ error) {
	if set == nil {
		return
	}
	for _, s := range set.Selections {
		switch sel := s.(type) {
		case *ast.Field:
			fld, err := convField(sel, labels)
			if err != nil {
				return nil, false, err
			}
			if fld.Via == quad.IRI(AnyKey) {
				if len(set.Selections) != 1 {
					return nil, false, fmt.Errorf("expand all cannot be used with other fields")
				} else if len(fld.Has) != 0 || len(fld.Fields) != 0 {
					return nil, false, fmt.Errorf("filters inside expand all are not supported")
				}
				return nil, true, nil
			}
			out = append(out, fld)
		default:
			return nil, false, fmt.Errorf("unknown selection type: %T", s)
		}
	}
	return
}

func stringToVia(s string) (_ quad.IRI, rev bool) {
	if len(s) > 0 && s[0] == '~' {
		rev = true
		s = s[1:]
	}
	if len(s) > 2 && s[0] == '<' && s[len(s)-1] == '>' {
		s = s[1 : len(s)-1]
	}
	return quad.IRI(s), rev
}

func argsToHas(dst []has, args []*ast.Argument, rev bool, labels []quad.Value) (out []has, err error) {
	out = dst
	for _, arg := range args {
		var vals []quad.Value
		vals, err = convValue(arg.Value)
		if err != nil {
			return
		}
		h := has{Values: vals, Labels: labels}
		h.Via, h.Rev = stringToVia(arg.Name.Value)
		h.Rev = h.Rev != rev
		out = append(out, h)
	}
	return
}

func convField(fld *ast.Field, labels []quad.Value) (out field, err error) {
	out.Labels = labels
	name := fld.Name.Value
	if fld.Alias != nil && fld.Alias.Value != "" {
		out.Alias = fld.Alias.Value
	} else {
		out.Alias = name
	}
	out.Via, out.Rev = stringToVia(name)
	// first check for "label" directive - it will affect all traversals
	for _, d := range fld.Directives {
		if d.Name == nil {
			continue
		}
		switch d.Name.Value {
		case "label":
			if len(d.Arguments) == 0 {
				out.Labels = nil
			} else if len(d.Arguments) > 1 {
				return out, fmt.Errorf("label directive should have 0 or 1 argument")
			} else if a := d.Arguments[0]; a.Name == nil || a.Name.Value != "v" {
				return out, fmt.Errorf("label directive should have 'v' argument")
			} else {
				vals, err := convValue(a.Value)
				if err != nil {
					return out, fmt.Errorf("error parsing label: %v", err)
				}
				out.Labels = vals
			}
		}
	}
	for _, d := range fld.Directives {
		if d.Name == nil {
			continue
		}
		switch d.Name.Value {
		case "rev", "reverse":
			if len(d.Arguments) == 0 {
				out.Rev = out.Rev != true
			} else {
				out.Has, err = argsToHas(out.Has, d.Arguments, true, out.Labels)
				if err != nil {
					return
				}
			}
		case "opt", "optional":
			out.Opt = true
		case "label":
			// already processed
		case "unnest":
			out.UnNest = true
		default:
			return out, fmt.Errorf("unknown directive: %q", d.Name.Value)
		}
	}
	out.Fields, out.AllFields, err = setToFields(fld.SelectionSet, out.Labels)
	if err != nil {
		return
	}
	out.Has, err = argsToHas(out.Has, fld.Arguments, false, out.Labels)
	if err != nil {
		return
	}
	return
}

func convValue(v ast.Value) (out []quad.Value, _ error) {
	switch v := v.(type) {
	case *ast.EnumValue:
		s := v.Value
		if len(s) > 2 && s[0] == '<' && s[len(s)-1] == '>' {
			s = s[1 : len(s)-1]
		}
		if len(s) > 2 && s[0] == '_' && s[1] == ':' {
			return []quad.Value{quad.BNode(s[2:])}, nil
		}
		return []quad.Value{quad.IRI(s)}, nil
	case *ast.StringValue:
		return []quad.Value{quad.StringToValue(v.Value)}, nil
	case *ast.IntValue:
		pv, _ := strconv.Atoi(v.Value)
		return []quad.Value{quad.Int(pv)}, nil
	case *ast.FloatValue:
		pv, _ := strconv.ParseFloat(v.Value, 64)
		return []quad.Value{quad.Float(pv)}, nil
	case *ast.BooleanValue:
		return []quad.Value{quad.Bool(v.Value)}, nil
	case *ast.ListValue:
		for _, sv := range v.Values {
			cv, err := convValue(sv)
			if err != nil {
				return nil, err
			} else if len(cv) != 1 {
				return nil, fmt.Errorf("unexpected value array in list: %v (%d)", cv, len(cv))
			}
			out = append(out, cv[0])
		}
		return
	default:
		return nil, fmt.Errorf("unsupported value type: %T", v)
	}
}
