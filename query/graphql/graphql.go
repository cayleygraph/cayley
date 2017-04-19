package graphql

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"

	"github.com/dennwc/graphql/language/ast"
	"github.com/dennwc/graphql/language/lexer"
	"github.com/dennwc/graphql/language/parser"
	"golang.org/x/net/context"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/path"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/query"
)

const Name = "graphql"

func init() {
	lexer.AllowNameRunes = "/.:<>~-" // TODO(dennwc): change to anonymous function with full IRI charset

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

type resultMap map[string]interface{}

func (resultMap) Err() error            { return nil }
func (m resultMap) Result() interface{} { return map[string]interface{}(m) }

type Session struct {
	qs graph.QuadStore
}

func (s *Session) Execute(ctx context.Context, qu string, out chan query.Result, limit int) {
	q, err := Parse(strings.NewReader(qu))
	if err != nil {
		select {
		case out <- query.ErrorResult(err):
		case <-ctx.Done():
		}
		return
	}
	m, err := q.Execute(ctx, s.qs)
	var r query.Result
	if err != nil {
		r = query.ErrorResult(err)
	} else {
		r = resultMap(m)
	}
	select {
	case out <- r:
	case <-ctx.Done():
	}
}
func (s *Session) FormatREPL(result query.Result) string {
	data, _ := json.MarshalIndent(result, "", "   ")
	return string(data)
}

// Configurable keywords and special field names.
var (
	ValueKey = "id"
	LimitKey = "first"
	SkipKey  = "offset"
)

type Query struct {
	fields []field
}

type has struct {
	Via    quad.IRI
	Rev    bool
	Values []quad.Value
}

type field struct {
	Via    quad.IRI
	Alias  string
	Rev    bool
	Opt    bool
	Has    []has
	Fields []field
}

func (f field) isSave() bool { return len(f.Has)+len(f.Fields) == 0 }

type object struct {
	id     graph.Value
	fields map[string][]graph.Value
}

func iterateObject(ctx context.Context, qs graph.QuadStore, f *field, p *path.Path) (out []map[string]interface{}, _ error) {
	var (
		limit = -1
		skip  = 0
	)
	for _, h := range f.Has {
		switch h.Via {
		case quad.IRI(ValueKey):
			p = p.Is(h.Values...)
		case quad.IRI(LimitKey), quad.IRI(SkipKey):
			if len(h.Values) != 1 {
				return nil, fmt.Errorf("unexpected arguments: %v (%d)", h.Values, len(h.Values))
			}
			n, ok := h.Values[0].(quad.Int)
			if !ok {
				return nil, fmt.Errorf("unexpected value type: %T", h.Values[0])
			}
			if h.Via == quad.IRI(LimitKey) {
				limit = int(n)
			} else {
				skip = int(n)
				if skip < 0 {
					skip = 0
				}
			}
		default:
			if h.Rev {
				p = p.HasReverse(h.Via, h.Values...)
			} else {
				p = p.Has(h.Via, h.Values...)
			}
		}
	}
	for _, f2 := range f.Fields {
		if f2.isSave() {
			if f2.Via == quad.IRI(ValueKey) {
				p = p.Tag(f2.Alias)
			} else {
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
			}
		}
	}
	if skip > 0 {
		p = p.Skip(int64(skip))
	}
	if limit >= 0 {
		p = p.Limit(int64(limit))
	}

	// load object ids and flat keys
	it, _ := p.BuildIterator().Optimize()
	it, _ = qs.OptimizeIterator(it)
	defer it.Close()

	var results []object
	for i := 0; limit < 0 || i < limit; i++ {
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		default:
		}
		if !it.Next() {
			break
		}
		tags := make(map[string]graph.Value)
		it.TagResults(tags)
		obj := object{id: it.Result()}
		if len(tags) > 0 {
			obj.fields = make(map[string][]graph.Value)
		}
		for k, v := range tags {
			obj.fields[k] = []graph.Value{v}
		}
		for it.NextPath() {
			select {
			case <-ctx.Done():
				return out, ctx.Err()
			default:
			}
			tags := make(map[string]graph.Value)
			it.TagResults(tags)
		dedup:
			for k, v := range tags {
				vals := obj.fields[k]
				for _, v2 := range vals {
					if graph.ToKey(v) == graph.ToKey(v2) {
						continue dedup
					}
				}
				obj.fields[k] = append(vals, v)
			}
		}
		results = append(results, obj)
	}

	// load values and complex keys
	for _, r := range results {
		obj := make(map[string]interface{})
		for k, arr := range r.fields {
			var vals []quad.Value
			for _, v := range arr {
				vals = append(vals, qs.NameOf(v))
			}
			if len(vals) == 1 {
				obj[k] = vals[0]
			} else {
				obj[k] = vals
			}
		}
		for _, f2 := range f.Fields {
			if f2.isSave() {
				continue
			}
			p := path.StartPathNodes(qs, r.id)
			if f2.Rev {
				p = p.In(f2.Via)
			} else {
				p = p.Out(f2.Via)
			}
			arr, err := iterateObject(ctx, qs, &f2, p)
			if err != nil {
				return out, err
			}
			var v interface{}
			if len(arr) == 1 {
				v = arr[0]
			} else if len(arr) > 1 {
				v = arr
			}
			obj[f2.Alias] = v
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
	fields, err := setToFields(def.SelectionSet)
	if err != nil {
		return nil, err
	}
	return &Query{fields: fields}, nil
}

func setToFields(set *ast.SelectionSet) (out []field, _ error) {
	if set == nil {
		return
	}
	for _, s := range set.Selections {
		switch sel := s.(type) {
		case *ast.Field:
			fld, err := convField(sel)
			if err != nil {
				return nil, err
			}
			out = append(out, fld)
		default:
			return nil, fmt.Errorf("unknown selection type: %T", s)
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

func argsToHas(dst []has, args []*ast.Argument, rev bool) (out []has, err error) {
	out = dst
	for _, arg := range args {
		var vals []quad.Value
		vals, err = convValue(arg.Value)
		if err != nil {
			return
		}
		h := has{Values: vals}
		h.Via, h.Rev = stringToVia(arg.Name.Value)
		h.Rev = h.Rev != rev
		out = append(out, h)
	}
	return
}

func convField(fld *ast.Field) (out field, err error) {
	name := fld.Name.Value
	if fld.Alias != nil && fld.Alias.Value != "" {
		out.Alias = fld.Alias.Value
	} else {
		out.Alias = name
	}
	out.Via, out.Rev = stringToVia(name)
	out.Fields, err = setToFields(fld.SelectionSet)
	if err != nil {
		return
	}
	out.Has, err = argsToHas(out.Has, fld.Arguments, false)
	if err != nil {
		return
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
				out.Has, err = argsToHas(out.Has, d.Arguments, true)
				if err != nil {
					return
				}
			}
		case "opt", "optional":
			out.Opt = true
		}
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
