package bolt

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"reflect"
	"sort"
	"strings"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/voc/rdf"
	"github.com/gorilla/websocket"
)

func NewServer(qs graph.QuadStore, lang string) (*Server, error) {
	l := query.GetLanguage(lang)
	if l == nil {
		return nil, fmt.Errorf("query language %q is not registered", lang)
	}
	return &Server{qs: qs, lang: l}, nil
}

type Server struct {
	qs   graph.QuadStore
	lang *query.Language
}

func (s *Server) newConn(c *websocket.Conn) *Conn {
	return &Conn{s: s, c: c}
}

func (s *Server) ListenAndServe(addr string) error {
	srv := &http.Server{
		Addr: addr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c, err := websocket.Upgrade(w, r, nil, 0, 0)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(err.Error()))
				return
			}
			defer c.Close()
			conn := s.newConn(c)
			if err = conn.Handle(); err != nil {
				log.Println(err)
			}
			log.Println("connection closed")
		}),
	}
	return srv.ListenAndServe()
}

type Conn struct {
	s       *Server
	c       *websocket.Conn
	buf     bytes.Buffer
	vers    int
	cbuf    [math.MaxUint16]byte
	failure int

	query struct {
		stop    func()
		nodes   map[int64]*Node
		fields  []string
		buf     []query.Result
		rels    []*Relationship
		results <-chan query.Result
	}
}

func (c *Conn) Read(p []byte) (int, error) {
	if c.buf.Len() == 0 {
		p, err := c.readBinary()
		if err != nil {
			return 0, err
		}
		c.buf.Write(p)
	}
	return c.buf.Read(p)
}

func (c *Conn) readBinary() ([]byte, error) {
	tp, p, err := c.c.ReadMessage()
	if err != nil {
		return nil, err
	} else if tp != websocket.BinaryMessage {
		return nil, fmt.Errorf("expected binary message")
	}
	return p, nil
}

var (
	magic = [...]byte{0x60, 0x60, 0xb0, 0x17}
	order = binary.BigEndian
)

const (
	curVersion = 1
)

func (c *Conn) Write(p []byte) (int, error) {
	if err := c.c.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *Conn) handshake() error {
	header := make([]byte, len(magic)+4*4)
	if _, err := io.ReadFull(c, header); err != nil {
		return err
	} else if !bytes.HasPrefix(header, magic[:]) {
		return fmt.Errorf("not a bolt protocol: %x", header[:len(magic)])
	}
	for i := 0; i < 4; i++ {
		vers := int(order.Uint32(header))
		if vers == curVersion {
			c.vers = vers
			_, err := c.Write(header[:4])
			return err
		}
		header = header[4:]
	}
	// no version
	_, _ = c.Write(make([]byte, 4))
	return fmt.Errorf("version negotiation failed")
}

func (c *Conn) Handle() error {
	if err := c.handshake(); err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}
	m, err := c.nextMessage()
	if err != nil {
		return err
	}
	in, ok := m.(*InitMsg)
	if !ok {
		return fmt.Errorf("expected INIT, got: %#v", m)
	}
	_ = in // TODO: auth
	if err = c.success(map[string]interface{}{
		"server": "Neo4j/3.1.0",
	}); err != nil {
		return err
	}

	for {
		m, err := c.nextMessage()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		if err = c.handleMsg(m); err != nil {
			return err
		}
	}
}

func (c *Conn) idFor(v graph.Value) int64 {
	qv := c.s.qs.NameOf(v)
	// TODO: real id
	return int64(order.Uint64(quad.HashOf(qv)))
}

func (c *Conn) toNode(v graph.Value) *Node {
	id := c.idFor(v)
	if nd, ok := c.query.nodes[int64(id)]; ok {
		return nd
	}
	qv := c.s.qs.NameOf(v)
	name := quad.ToString(qv)
	nd := &Node{
		ID: int64(id),
		Fields: map[string]interface{}{
			"id": name,
		},
	}
	{
		it := c.s.qs.QuadIterator(quad.Subject, v)
		defer it.Close()
		ctx := context.TODO()
		for it.Next(ctx) {
			q := c.s.qs.Quad(it.Result())
			if q.Predicate == quad.IRI(rdf.Type) || q.Predicate == quad.IRI(rdf.Type).Full() {
				nd.Labels = append(nd.Labels, quad.ToString(q.Object))
				continue
			}
			switch q.Object.(type) {
			case quad.IRI, quad.BNode:
				c.query.rels = append(c.query.rels, &Relationship{
					ID:   rand.Int63(),
					From: id, To: c.idFor(c.s.qs.ValueOf(q.Object)),
					Type: quad.ToString(q.Predicate),
				})
			default:
				nd.Fields[quad.ToString(q.Predicate)] = c.resultToValue(q.Object)
			}
		}
	}
	if c.query.nodes == nil {
		c.query.nodes = make(map[int64]*Node)
	}
	c.query.nodes[nd.ID] = nd
	return nd
}
func (c *Conn) resultToValue(v interface{}) interface{} {
	switch v := v.(type) {
	case graph.Value:
		return c.toNode(v)
	case quad.IRI:
		return string(v)
	case quad.Value:
		return v.Native()
	case map[string]graph.Value:
		out := make(map[string]interface{}, len(v))
		for k, s := range v {
			out[k] = c.resultToValue(s)
		}
		if nd, ok := out["id"].(*Node); ok {
			for k, v := range out {
				if k == "id" {
					continue
				} else if nd2, ok := v.(*Node); ok {
					c.query.rels = append(c.query.rels, &Relationship{
						ID: rand.Int63(), From: nd.ID, To: nd2.ID,
						Type: k,
					})
					continue
				}
				nd.Fields[k] = v
			}
			return nd
		}
		return out
	default:
		return v
	}
}

func (c *Conn) pullAll() error {
	defer c.closeQuery()
	send := func(r query.Result) error {
		if err := r.Err(); err != nil {
			return c.failuref("%v", err)
		}
		v := r.Result()
		v = c.resultToValue(v)
		var rec Record
		if m, ok := v.(map[string]interface{}); ok && false {
			for _, name := range c.query.fields {
				rec.Fields = append(rec.Fields, m[name])
			}
		} else {
			rec.Fields = []interface{}{v}
		}
		if err := c.sendMessage(&rec); err != nil {
			return err
		}
		for _, rel := range c.query.rels {
			if err := c.sendMessage(rel); err != nil {
				return err
			}
		}
		c.query.rels = c.query.rels[:0]
		return nil
	}
	for _, r := range c.query.buf {
		if err := send(r); err != nil {
			return err
		}
	}
	c.query.buf = c.query.buf[:0]
	for r := range c.query.results {
		if err := send(r); err != nil {
			return err
		}
	}
	return c.success(nil)
}

func (c *Conn) handleMsg(m interface{}) error {
	switch m := m.(type) {
	case *RunMsg:
		st := strings.TrimSpace(m.Statement)
		if strings.HasPrefix(st, "CALL ") {
			st = strings.TrimSpace(strings.TrimPrefix(st, "CALL "))
			switch st {
			case "db.indexes()":
				c.emptyResults()
				return c.success(nil)
			}
			return c.failuref("this call is not supported")
		} else if strings.HasPrefix(st, "EXPLAIN ") {
			st = strings.TrimSpace(strings.TrimPrefix(st, "EXPLAIN "))
			return c.failuref("explain is not supported")
		}
		return c.handleQuery(m.Statement, m.Parameters)
	case *PullAllMsg:
		if c.query.results == nil {
			return c.failuref("no active queries")
		}
		return c.pullAll()
	case *DiscardAllMsg:
		c.closeQuery()
		return c.success(nil)
	default:
		return c.failuref("not implemented")
	}
}

func (c *Conn) emptyResults() {
	out := make(chan query.Result)
	close(out)
	c.query.results = out
	c.query.stop = func() {}
}

func (c *Conn) closeQuery() {
	if c.query.results != nil {
		c.query.stop()
		c.query.fields = c.query.fields[:0]
		c.query.results = nil
		c.query.stop = nil
		c.query.nodes = nil
		c.query.rels = nil
	}
}

func (c *Conn) handleQuery(qu string, params map[string]interface{}) error {
	sess := c.s.lang.Session(c.s.qs)
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan query.Result, 10)
	go sess.Execute(ctx, qu, out, 0)
	c.query.buf = c.query.buf[:0]
	c.query.results, c.query.stop = out, cancel
	// wait for first result to send fields
	r, ok := <-out
	if !ok {
		// no results
		c.closeQuery()
		return c.success(nil)
	} else if err := r.Err(); err != nil {
		c.closeQuery()
		return c.failuref("%v", err)
	}
	if m, ok := r.Result().(map[string]graph.Value); ok && false {
		c.query.fields = c.query.fields[:0]
		for k := range m {
			c.query.fields = append(c.query.fields, k)
		}
		sort.Strings(c.query.fields)
	} else {
		c.query.fields = []string{"val"}
	}
	c.query.buf = append(c.query.buf, r)
	return c.success(map[string]interface{}{
		"fields": c.query.fields,
	})
}

func (c *Conn) readChunk() ([]byte, error) {
	var sb [2]byte
	_, err := c.Read(sb[:])
	if err != nil {
		return nil, err
	}
	sz := order.Uint16(sb[:])
	if sz == 0 {
		return nil, nil
	}
	buf := make([]byte, sz)
	if _, err = io.ReadFull(c, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func (c *Conn) readMessage() ([]byte, error) {
	var out []byte
	for {
		p, err := c.readChunk()
		out = append(out, p...)
		if err != nil || len(p) == 0 {
			return out, err
		}
	}
}

func (c *Conn) nextMessage() (interface{}, error) {
	for {
		p, err := c.readMessage()
		if err != nil {
			return nil, err
		} else if len(p) == 0 {
			return nil, fmt.Errorf("empty message")
		}
		o, _, err := decodeMsg(p)
		if err != nil {
			log.Printf("bolt msg err: %v", err)
			return nil, err
		}
		//log.Printf("<-- %#v", o)
		switch o.(type) {
		case *AckFailureMsg:
			if c.failure <= 0 {
				return nil, fmt.Errorf("no failure to acknowledge")
			}
			c.failure--
			if err := c.success(nil); err != nil {
				return nil, err
			}
		case *ResetMsg:
			c.failure = 0
			c.closeQuery()
			if err := c.success(nil); err != nil {
				return nil, err
			}
		default:
			if c.failure == 0 {
				// return the message
				return o, nil
			}
			// ignore everything until ack or reset
			if err := c.ignored(); err != nil {
				return nil, err
			}
		}
	}
}

func (c *Conn) sendMessage(o interface{}) error {
	//log.Printf("--> %#v", o)
	data, err := encodeMsg(o)
	if err != nil {
		return err
	}
	for len(data) > 0 {
		n := len(data)
		if n > len(c.cbuf)-2 {
			n = len(c.cbuf) - 2
		}
		order.PutUint16(c.cbuf[:2], uint16(n))
		n = copy(c.cbuf[2:], data)
		data = data[n:]
		if _, err = c.Write(c.cbuf[:2+n]); err != nil {
			return err
		}
	}
	order.PutUint16(c.cbuf[:2], uint16(0))
	_, err = c.Write(c.cbuf[:2])
	return err
}

func (c *Conn) success(m map[string]interface{}) error {
	return c.sendMessage(&SuccessMsg{Metadata: m})
}

func (c *Conn) ignored() error {
	return c.sendMessage(&IgnoredMsg{Metadata: nil})
}

func (c *Conn) failuref(format string, args ...interface{}) error {
	c.failure++
	return c.sendMessage(&FailureMsg{Metadata: map[string]interface{}{
		"code": "error", "message": fmt.Sprintf(format, args...),
	}})
}

const (
	typString = 0x80
	typList   = 0x90
	typMap    = 0xa0
	typStruct = 0xb0
)

func encodeMsg(o interface{}) ([]byte, error) {
	switch v := o.(type) {
	case nil:
		return []byte{0xc0}, nil
	case bool:
		if v {
			return []byte{0xc3}, nil
		}
		return []byte{0xc2}, nil
	case int64:
		buf := make([]byte, 1+8)
		buf[0] = 0xcb // TODO: optimal size
		order.PutUint64(buf[1:], uint64(v))
		return buf, nil
	case int:
		buf := make([]byte, 1+8)
		buf[0] = 0xcb // TODO: optimal size
		order.PutUint64(buf[1:], uint64(v))
		return buf, nil
	case string:
		buf := make([]byte, 1+4+len(v))
		buf[0] = 0xd2 // TODO: optimal size
		order.PutUint32(buf[1:], uint32(len(v)))
		copy(buf[5:], v)
		return buf, nil
	case []string:
		arr := make([]interface{}, 0, len(v))
		for _, s := range v {
			arr = append(arr, s)
		}
		return encodeMsg(arr)
	case []interface{}:
		buf := make([]byte, 1+4)
		buf[0] = 0xd6 // TODO: optimal size
		order.PutUint32(buf[1:], uint32(len(v)))
		for _, s := range v {
			sub, err := encodeMsg(s)
			if err != nil {
				return nil, err
			}
			buf = append(buf, sub...)
		}
		return buf, nil
	case map[string]interface{}:
		buf := make([]byte, 1+4)
		buf[0] = 0xda // TODO: optimal size
		order.PutUint32(buf[1:], uint32(len(v)))
		for k, s := range v {
			sub, err := encodeMsg(k)
			if err != nil {
				return nil, err
			}
			buf = append(buf, sub...)

			sub, err = encodeMsg(s)
			if err != nil {
				return nil, err
			}
			buf = append(buf, sub...)
		}
		return buf, nil
	case Msg:
		return encodeStruct(v)
	default:
		return nil, fmt.Errorf("unsupported type: %T", o)
	}
}

func encodeStruct(o Msg) ([]byte, error) {
	rv := reflect.ValueOf(o)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	rt := rv.Type()

	buf := make([]byte, 1+2+1)
	buf[0] = 0xdd // TODO: optimal size
	order.PutUint16(buf[1:], uint16(rt.NumField()))
	buf[3] = o.Signature()

	for i := 0; i < rt.NumField(); i++ {
		fv := rv.Field(i)
		sub, err := encodeMsg(fv.Interface())
		if err != nil {
			return nil, err
		}
		buf = append(buf, sub...)
	}
	return buf, nil
}

func decodeMsg(p []byte) (interface{}, int, error) {
	if len(p) == 0 {
		return nil, 0, io.ErrUnexpectedEOF
	}
	var mv int
	move := func(n int) {
		mv += n
		p = p[n:]
	}
	tp := p[0]
	move(1)
	// fast path for one byte types
	switch tp {
	case 0xc0: // Null
		return nil, mv, nil
	case 0xc2: // false
		return false, mv, nil
	case 0xc3: // true
		return true, mv, nil
	}
	if tp&0x80 == 0 { // TINY_INT (+)
		return int(tp), mv, nil
	} else if tp&0xf0 == 0xf0 {
		return int(tp), mv, nil // TINY_INT (-)
	}
	// other ints
	switch tp {
	case 0xc8: // INT_8
		v := p[0]
		move(1)
		return int(v), mv, nil
	case 0xc9: // INT_16
		v := int(order.Uint16(p))
		move(2)
		return v, mv, nil
	case 0xca: // INT_32
		v := int(order.Uint32(p))
		move(4)
		return v, mv, nil
	case 0xcb: // INT_64
		v := int(order.Uint64(p))
		move(8)
		return v, mv, nil
	}
	sz, typ := 0, 0
	// small types
	hi, lo := tp&0xf0, tp&0x0f
	for _, t := range []byte{
		typString,
		typList,
		typMap,
		typStruct,
	} {
		if hi == t {
			typ = int(t)
			sz = int(lo)
			break
		}
	}
	if typ == 0 {
		// value type
		switch tp {
		case 0xd0, 0xd1, 0xd2:
			typ = typString
		case 0xd4, 0xd5, 0xd6:
			typ = typList
		case 0xd8, 0xd9, 0xda:
			typ = typMap
		case 0xdc, 0xdd:
			typ = typStruct
		}
		switch tp {
		case 0xd0, 0xd4, 0xd8, 0xdc: // 8bit size
			sz = int(p[0])
			move(1)
		case 0xd1, 0xd5, 0xd9, 0xdd: // 16bit size
			sz = int(order.Uint16(p))
			move(2)
		case 0xd2, 0xd6, 0xda: // 32bit size
			sz = int(order.Uint32(p))
			move(4)
		}
	}
	switch typ {
	case typString:
		if sz > len(p) {
			return nil, 0, io.ErrUnexpectedEOF
		}
		v := string(p[:sz])
		move(sz)
		return v, mv, nil
	case typList:
		out := make([]interface{}, 0, sz)
		for i := 0; i < sz; i++ {
			v, dn, err := decodeMsg(p)
			if err != nil {
				return out, mv, err
			}
			move(dn)
			out = append(out, v)
		}
		return out, mv, nil
	case typMap:
		// decode first KV to detect key type
		if sz == 0 {
			return map[string]interface{}{}, mv, nil
		}

		kv := func() (k, v interface{}, err error) {
			var dn int
			k, dn, err = decodeMsg(p)
			if err != nil {
				return
			}
			move(dn)

			v, dn, err = decodeMsg(p)
			if err != nil {
				return
			}
			move(dn)
			return
		}

		k, v, err := kv()
		if err != nil {
			return nil, mv, err
		}
		switch k.(type) {
		case string:
			out := make(map[string]interface{}, sz)
			out[k.(string)] = v
			for i := 1; i < sz; i++ {
				k, v, err = kv()
				if err != nil {
					return out, mv, err
				}
				out[k.(string)] = v
			}
			return out, mv, nil
		case int:
			out := make(map[int]interface{}, sz)
			out[k.(int)] = v
			for i := 1; i < sz; i++ {
				k, v, err = kv()
				if err != nil {
					return out, mv, err
				}
				out[k.(int)] = v
			}
			return out, mv, nil
		default:
			return nil, mv, fmt.Errorf("unsupported map key type: %T", k)
		}
	case typStruct:
		sig := p[0]
		move(1)
		var o Msg
		if t, ok := signatures[sig]; ok {
			o = reflect.New(reflect.TypeOf(t)).Interface().(Msg)
		} else {
			return nil, 0, fmt.Errorf("unknown struct signture: %x", sig)
		}
		dn, err := decodeStruct(p, o)
		if err != nil {
			return nil, 0, err
		}
		move(dn)
		return o, mv, nil
	default:
		return nil, 0, fmt.Errorf("cannot decode marker %x (type: %x, size %d)", tp, typ, sz)
	}
}

func decodeStruct(p []byte, o Msg) (int, error) {
	rv := reflect.ValueOf(o).Elem()
	rt := rv.Type()
	if rt.Kind() != reflect.Struct {
		return 0, fmt.Errorf("unsupported type: %v", rt)
	}
	mv := 0
	move := func(n int) {
		mv += n
		p = p[n:]
	}
	for i := 0; i < rt.NumField(); i++ {
		v, dn, err := decodeMsg(p)
		if err != nil {
			return mv, err
		}
		move(dn)
		fv := rv.Field(i)
		fv.Set(reflect.ValueOf(v))
	}
	return mv, nil
}
