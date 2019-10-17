package gephi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"

	"github.com/julienschmidt/httprouter"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/cayley/query/shape"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc/rdf"
	"github.com/cayleygraph/quad/voc/rdfs"
	"github.com/cayleygraph/quad/voc/schema"
)

const (
	defaultLimit = 10000
	defaultSize  = 20
	limitCoord   = 500
)

const (
	iriInlinePred = quad.IRI("gephi:inline")
	iriPosX       = quad.IRI("gephi:x")
	iriPosY       = quad.IRI("gephi:y")
)

var defaultInline = []quad.IRI{
	iriPosX, iriPosY,

	rdf.Type,
	rdfs.Label,
	schema.Name,
	schema.UrlProp,
}

type GraphStreamHandler struct {
	QS graph.QuadStore
}

type valHash [quad.HashSize]byte

type GraphStream struct {
	seen map[valHash]int
	buf  *bytes.Buffer
	w    io.Writer
}

func printNodeID(id int) string {
	return strconv.FormatInt(int64(id), 16)
}

func NewGraphStream(w io.Writer) *GraphStream {
	return &GraphStream{
		w:    w,
		seen: make(map[valHash]int),
		buf:  bytes.NewBuffer(nil),
	}
}
func toNodeLabel(v quad.Value) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v.Native())
}

func randCoord() float64 {
	return (rand.Float64() - 0.5) * limitCoord * 2
}
func randPos() (x float64, y float64) {
	x = randCoord()
	x2 := x * x
	for y = randCoord(); x2+y*y > limitCoord*limitCoord; y = randCoord() {
	}
	return
}

func setStringProp(v *string, props map[quad.Value]quad.Value, name quad.IRI) {
	if p, ok := props[name]; ok {
		if s, ok := p.Native().(string); ok {
			*v = s
		}
	}
}

func (gs *GraphStream) makeOneNode(id string, v quad.Value, props map[quad.Value]quad.Value) map[string]streamNode {
	x, y := randPos()
	var xok, yok bool
	if p, ok := props[iriPosX]; ok {
		xok = true
		switch p := p.(type) {
		case quad.Int:
			x = float64(p)
		case quad.Float:
			x = float64(p)
		default:
			xok = false
		}
	}
	if p, ok := props[iriPosY]; ok {
		yok = true
		switch p := p.(type) {
		case quad.Int:
			y = float64(p)
		case quad.Float:
			y = float64(p)
		default:
			yok = false
		}
	}
	var slabel string
	setStringProp(&slabel, props, rdfs.Label)
	setStringProp(&slabel, props, schema.Name)

	var label interface{}
	if slabel != "" {
		label = slabel
	} else {
		label = v.Native()
	}

	node := streamNode{
		"label": label,
		"size":  defaultSize, "x": x, "y": y,
	}
	for k, v := range props {
		if k == nil || v == nil ||
			(k == iriPosX && xok) ||
			(k == iriPosY && yok) {
			continue
		}
		node[toNodeLabel(k)] = toNodeLabel(v)
	}
	return map[string]streamNode{id: node}
}
func (gs *GraphStream) AddNode(v quad.Value, props map[quad.Value]quad.Value) string {
	var h valHash
	quad.HashTo(v, h[:])
	return gs.addNode(v, h, props)
}
func (gs *GraphStream) encode(o interface{}) {
	data, _ := json.Marshal(o)
	gs.buf.Write(data)
	// Gephi requires \r character at the end of each line
	gs.buf.WriteString("\r\n")
}
func (gs *GraphStream) addNode(v quad.Value, h valHash, props map[quad.Value]quad.Value) string {
	id, ok := gs.seen[h]
	if ok {
		return printNodeID(id)
	} else if v == nil {
		return ""
	}
	id = len(gs.seen)
	gs.seen[h] = id
	sid := printNodeID(id)

	m := gs.makeOneNode(sid, v, props)
	gs.encode(graphStreamEvent{AddNodes: m})
	return sid
}
func (gs *GraphStream) ChangeNode(v quad.Value, sid string, props map[quad.Value]quad.Value) {
	m := gs.makeOneNode(sid, v, props)
	gs.encode(graphStreamEvent{ChangeNodes: m})
}
func (gs *GraphStream) AddEdge(i int, s, o string, p quad.Value) {
	id := "q" + strconv.FormatInt(int64(i), 16)
	ps := toNodeLabel(p)
	gs.encode(graphStreamEvent{
		AddEdges: map[string]streamEdge{id: {
			Subject:   s,
			Predicate: ps, Label: ps,
			Object: o,
		}},
	})
}
func (gs *GraphStream) Flush() error {
	if gs.buf.Len() == 0 {
		return nil
	}
	_, err := gs.buf.WriteTo(gs.w)
	if err == nil {
		gs.buf.Reset()
	}
	return err
}

type streamNode map[string]interface{}
type streamEdge struct {
	Subject   string `json:"source"`
	Label     string `json:"label"`
	Predicate string `json:"pred"`
	Object    string `json:"target"`
}
type graphStreamEvent struct {
	AddNodes    map[string]streamNode `json:"an,omitempty"`
	ChangeNodes map[string]streamNode `json:"cn,omitempty"`
	DelNodes    map[string]streamNode `json:"dn,omitempty"`

	AddEdges    map[string]streamEdge `json:"ae,omitempty"`
	ChangeEdges map[string]streamEdge `json:"ce,omitempty"`
	DelEdges    map[string]streamEdge `json:"de,omitempty"`
}

func (s *GraphStreamHandler) serveRawQuads(ctx context.Context, gs *GraphStream, quads shape.Shape, limit int) {
	it := shape.BuildIterator(ctx, s.QS, quads).Iterate()
	defer it.Close()

	var sh, oh valHash
	for i := 0; (limit < 0 || i < limit) && it.Next(ctx); i++ {
		qv := it.Result()
		if qv == nil {
			continue
		}
		q := s.QS.Quad(qv)
		quad.HashTo(q.Subject, sh[:])
		quad.HashTo(q.Object, oh[:])
		s, o := gs.addNode(q.Subject, sh, nil), gs.addNode(q.Object, oh, nil)
		if s == "" || o == "" {
			continue
		}
		gs.AddEdge(i, s, o, q.Predicate)
		if err := gs.Flush(); err != nil {
			return
		}
	}
}

func shouldInline(v quad.Value) bool {
	switch v.(type) {
	case quad.Bool, quad.Int, quad.Float:
		return true
	}
	return false
}

func (s *GraphStreamHandler) serveNodesWithProps(ctx context.Context, gs *GraphStream, limit int) {
	propsPath := path.NewPath(s.QS).Has(iriInlinePred, quad.Bool(true))

	// list of predicates marked as inline properties for gephi
	inline := make(map[quad.Value]struct{})
	err := propsPath.Iterate(ctx).EachValue(s.QS, func(v quad.Value) {
		inline[v] = struct{}{}
	})
	if err != nil {
		clog.Errorf("cannot iterate over properties: %v", err)
		return
	}
	// inline some well-known predicates
	for _, iri := range defaultInline {
		inline[iri] = struct{}{}
		inline[iri.Full()] = struct{}{}
	}

	ignore := make(map[quad.Value]struct{})

	nodes := iterator.NewNot(propsPath.BuildIterator(ctx), s.QS.NodesAllIterator())

	ictx, cancel := context.WithCancel(ctx)
	defer cancel()

	itc := iterator.Iterate(ictx, nodes).On(s.QS).Limit(limit)

	qi := 0
	_ = itc.EachValuePair(s.QS, func(v graph.Ref, nv quad.Value) {
		if _, skip := ignore[nv]; skip {
			return
		}
		// list of inline properties
		props := make(map[quad.Value]quad.Value)

		var (
			sid   string
			h, oh valHash
		)
		quad.HashTo(nv, h[:])

		predIt := s.QS.QuadIterator(quad.Subject, v).Iterate()
		defer predIt.Close()
		for predIt.Next(ictx) {
			// this check helps us ignore nodes with no links
			if sid == "" {
				sid = gs.addNode(nv, h, props)
			}
			q := s.QS.Quad(predIt.Result())
			if _, ok := inline[q.Predicate]; ok {
				props[q.Predicate] = q.Object
				ignore[q.Object] = struct{}{}
			} else if shouldInline(q.Object) {
				props[q.Predicate] = q.Object
			} else {
				quad.HashTo(q.Object, oh[:])
				o := gs.addNode(q.Object, oh, nil)
				if o == "" {
					continue
				}
				gs.AddEdge(qi, sid, o, q.Predicate)
				qi++
				if err := gs.Flush(); err != nil {
					cancel()
					return
				}
			}
		}
		if err := predIt.Err(); err != nil {
			cancel()
			return
		} else if sid == "" {
			return
		}
		if len(props) != 0 {
			gs.ChangeNode(nv, sid, props)
		}
		if err := gs.Flush(); err != nil {
			cancel()
			return
		}
	})
}

func valuesFromString(s string) []quad.Value {
	if s == "" {
		return nil
	}
	arr := strings.Split(s, ",")
	out := make([]quad.Value, 0, len(arr))
	for _, s := range arr {
		out = append(out, quad.StringToValue(s))
	}
	return out
}

func (s *GraphStreamHandler) ServeHTTP(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	ctx := context.TODO()
	var limit int
	if s := r.FormValue("limit"); s != "" {
		limit, _ = strconv.Atoi(s)
	}
	if limit == 0 {
		limit = defaultLimit
	}
	mode := "raw"
	if s := r.FormValue("mode"); s != "" {
		mode = s
	}

	w.Header().Set("Content-Type", "application/stream+json")
	gs := NewGraphStream(w)
	switch mode {
	case "nodes":
		s.serveNodesWithProps(ctx, gs, limit)
	case "raw":
		values := shape.FilterQuads(
			valuesFromString(r.FormValue("sub")),
			valuesFromString(r.FormValue("pred")),
			valuesFromString(r.FormValue("obj")),
			valuesFromString(r.FormValue("label")),
		)
		s.serveRawQuads(ctx, gs, values, limit)
	default:
		w.WriteHeader(http.StatusBadRequest)
		return
	}
}
