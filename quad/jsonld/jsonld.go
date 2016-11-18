// Package jsonld provides an encoder/decoder for JSON-LD quad format
package jsonld

import (
	"encoding/json"
	"fmt"
	"github.com/cayleygraph/cayley/quad"
	"github.com/linkeddata/gojsonld"
	"io"
)

// AutoConvertTypedString allows to convert TypedString values to native
// equivalents directly while parsing. It will call ToNative on all TypedString values.
//
// If conversion error occurs, it will preserve original TypedString value.
var AutoConvertTypedString = true

func init() {
	quad.RegisterFormat(quad.Format{
		Name:   "jsonld",
		Ext:    []string{".jsonld"},
		Mime:   []string{"application/ld+json"},
		Writer: func(w io.Writer) quad.WriteCloser { return NewWriter(w) },
		Reader: func(r io.Reader) quad.ReadCloser { return NewReader(r) },
	})
}

// NewReader returns quad reader for JSON-LD stream.
func NewReader(r io.Reader) *Reader {
	var o interface{}
	if err := json.NewDecoder(r).Decode(&o); err != nil {
		return &Reader{err: err}
	}
	return NewReaderFromMap(o)
}

// NewReaderFromMap returns quad reader for JSON-LD map object.
func NewReaderFromMap(o interface{}) *Reader {
	data, err := gojsonld.ToRDF(o, gojsonld.NewOptions(""))
	if err != nil {
		return &Reader{err: err}
	}
	return &Reader{
		graphs: data.Graphs,
	}
}

type Reader struct {
	err    error
	name   string
	n      int
	graphs map[string][]*gojsonld.Triple
}

func (r *Reader) ReadQuad() (quad.Quad, error) {
	if r.err != nil {
		return quad.Quad{}, r.err
	}
next:
	if len(r.graphs) == 0 {
		return quad.Quad{}, io.EOF
	}
	if r.name == "" {
		for gname, _ := range r.graphs {
			r.name = gname
			break
		}
	}
	if r.n >= len(r.graphs[r.name]) {
		r.n = 0
		delete(r.graphs, r.name)
		r.name = ""
		goto next
	}
	cur := r.graphs[r.name][r.n]
	r.n++
	var graph quad.Value
	if r.name != "@default" {
		graph = quad.IRI(r.name)
	}
	return quad.Quad{
		Subject:   toValue(cur.Subject),
		Predicate: toValue(cur.Predicate),
		Object:    toValue(cur.Object),
		Label:     graph,
	}, nil
}

func (r *Reader) Close() error {
	r.graphs = nil
	return r.err
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w, ds: gojsonld.NewDataset()}
}

type Writer struct {
	w   io.Writer
	ds  *gojsonld.Dataset
	ctx interface{}
}

func (w *Writer) SetLdContext(ctx interface{}) {
	w.ctx = ctx
}

func (w *Writer) WriteQuad(q quad.Quad) error {
	var graph string
	if q.Label == nil {
		graph = "@default"
	} else if iri, ok := q.Label.(quad.IRI); ok {
		graph = string(iri)
	} else {
		graph = q.Label.String()
	}
	g := w.ds.Graphs[graph]
	g = append(g, gojsonld.NewTriple(
		toTerm(q.Subject),
		toTerm(q.Predicate),
		toTerm(q.Object),
	))
	w.ds.Graphs[graph] = g
	return nil
}

func (w *Writer) Close() error {
	opts := gojsonld.NewOptions("")
	var data interface{}
	data = gojsonld.FromRDF(w.ds, opts)
	if w.ctx != nil {
		out, err := gojsonld.Compact(data, w.ctx, opts)
		if err != nil {
			return err
		}
		data = out
	}
	return json.NewEncoder(w.w).Encode(data)
}

func toTerm(v quad.Value) gojsonld.Term {
	switch v := v.(type) {
	case quad.IRI:
		return gojsonld.NewResource(string(v))
	case quad.BNode:
		return gojsonld.NewBlankNode(string(v))
	case quad.String:
		return gojsonld.NewLiteralWithDatatype(
			string(v),
			gojsonld.NewResource(gojsonld.XSD_STRING),
		)
	case quad.TypedString:
		return gojsonld.NewLiteralWithDatatype(
			string(v.Value),
			gojsonld.NewResource(string(v.Type)),
		)
	case quad.LangString:
		return gojsonld.NewLiteralWithLanguageAndDatatype(
			string(v.Value),
			string(v.Lang),
			gojsonld.NewResource(gojsonld.XSD_STRING),
		)
	case quad.TypedStringer:
		return toTerm(v.TypedString())
	default:
		return gojsonld.NewLiteralWithDatatype(v.String(), gojsonld.NewResource(gojsonld.XSD_STRING))
	}
}

func toValue(t gojsonld.Term) quad.Value {
	switch t := t.(type) {
	case *gojsonld.Resource:
		return quad.IRI(t.URI)
	case *gojsonld.BlankNode:
		return quad.BNode(t.ID)
	case *gojsonld.Literal:
		if t.Language != "" {
			return quad.LangString{
				Value: quad.String(t.Value),
				Lang:  t.Language,
			}
		} else if t.Datatype != nil {
			ts := quad.TypedString{
				Value: quad.String(t.Value),
				Type:  quad.IRI(t.Datatype.RawValue()),
			}
			if AutoConvertTypedString {
				if v, err := ts.ParseValue(); err == nil {
					return v
				}
			}
			return ts
		}
		return quad.String(t.Value)
	default:
		panic(fmt.Errorf("unexpected term type: %T", t))
	}
}
