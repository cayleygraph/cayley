package schema_test

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/schema"
	"github.com/cayleygraph/cayley/voc"
	"github.com/cayleygraph/cayley/voc/rdf"
)

type item struct {
	rdfType struct{} `quad:"rdf:type > some:item"`
	ID      quad.IRI `quad:"@id"`
	Name    string   `quad:"name"`
	Spec    string   `quad:"spec,optional"`
}

type item2 struct {
	Name string `quad:"name"`
	Spec string `quad:"spec"`
}

type treeItemByIRI []treeItem

func (o treeItemByIRI) Len() int           { return len(o) }
func (o treeItemByIRI) Less(i, j int) bool { return o[i].ID < o[j].ID }
func (o treeItemByIRI) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }

type treeItem struct {
	ID       quad.IRI   `quad:"@id"`
	Name     string     `quad:"name"`
	Children []treeItem `quad:"child"`
}

func (t *treeItem) Sort() {
	for _, c := range t.Children {
		c.Sort()
	}
	sort.Sort(treeItemByIRI(t.Children))
}

type treeItemOptByIRI []treeItemOpt

func (o treeItemOptByIRI) Len() int           { return len(o) }
func (o treeItemOptByIRI) Less(i, j int) bool { return o[i].ID < o[j].ID }
func (o treeItemOptByIRI) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }

type treeItemOpt struct {
	ID       quad.IRI      `quad:"@id"`
	Name     string        `quad:"name"`
	Children []treeItemOpt `quad:"child,optional"`
}

func (t *treeItemOpt) Sort() {
	for _, c := range t.Children {
		c.Sort()
	}
	sort.Sort(treeItemOptByIRI(t.Children))
}

type treeItemReqByIRI []treeItemReq

func (o treeItemReqByIRI) Len() int           { return len(o) }
func (o treeItemReqByIRI) Less(i, j int) bool { return o[i].ID < o[j].ID }
func (o treeItemReqByIRI) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }

type treeItemReq struct {
	ID       quad.IRI      `quad:"@id"`
	Name     string        `quad:"name"`
	Children []treeItemReq `quad:"child,required"`
}

func (t *treeItemReq) Sort() {
	for _, c := range t.Children {
		c.Sort()
	}
	sort.Sort(treeItemReqByIRI(t.Children))
}

type genObject struct {
	ID   quad.IRI `quad:"@id"`
	Name string   `quad:"name"`
}

type subObject struct {
	genObject
	Num int `quad:"num"`
}

func init() {
	voc.RegisterPrefix("ex:", "http://example.org/")
	schema.RegisterType(quad.IRI("ex:Coords"), Coords{})
}

type Coords struct {
	Lat float64 `json:"ex:lat"`
	Lng float64 `json:"ex:lng"`
}

func iri(s string) quad.IRI { return quad.IRI(s) }

const typeIRI = quad.IRI(rdf.Type)

var testWriteValueCases = []struct {
	name   string
	obj    interface{}
	id     quad.Value
	expect []quad.Quad
	err    error
}{
	{
		"complex object",
		struct {
			rdfType struct{} `quad:"rdf:type > some:Type"`
			ID      quad.IRI `quad:"@id"`
			Name    string   `quad:"name"`
			Values  []string `quad:"values"`
			Items   []item   `quad:"items"`
			Sub     *item    `quad:"sub"`
		}{
			ID:     "1234",
			Name:   "some item",
			Values: []string{"val1", "val2"},
			Items: []item{
				{ID: "sub1", Name: "Sub 1"},
				{ID: "sub2", Name: "Sub 2"},
			},
			Sub: &item{ID: "sub3", Name: "Sub 3"},
		},
		iri("1234"),
		[]quad.Quad{
			{iri("1234"), typeIRI, iri("some:Type"), nil},
			{iri("1234"), iri("name"), quad.String(`some item`), nil},
			{iri("1234"), iri("values"), quad.String(`val1`), nil},
			{iri("1234"), iri("values"), quad.String(`val2`), nil},

			{iri("sub1"), typeIRI, iri("some:item"), nil},
			{iri("sub1"), iri("name"), quad.String(`Sub 1`), nil},
			{iri("1234"), iri("items"), iri("sub1"), nil},

			{iri("sub2"), typeIRI, iri("some:item"), nil},
			{iri("sub2"), iri("name"), quad.String(`Sub 2`), nil},
			{iri("1234"), iri("items"), iri("sub2"), nil},

			{iri("sub3"), typeIRI, iri("some:item"), nil},
			{iri("sub3"), iri("name"), quad.String(`Sub 3`), nil},
			{iri("1234"), iri("sub"), iri("sub3"), nil},
		},
		nil,
	},
	{
		"complex object (embedded)",
		struct {
			rdfType struct{} `quad:"rdf:type > some:Type"`
			item2
			ID     quad.IRI `quad:"@id"`
			Values []string `quad:"values"`
		}{
			item2:  item2{Name: "Sub 1", Spec: "special"},
			ID:     "1234",
			Values: []string{"val1", "val2"},
		},
		iri("1234"),
		[]quad.Quad{
			{iri("1234"), typeIRI, iri("some:Type"), nil},
			{iri("1234"), iri("name"), quad.String(`Sub 1`), nil},
			{iri("1234"), iri("spec"), quad.String(`special`), nil},
			{iri("1234"), iri("values"), quad.String(`val1`), nil},
			{iri("1234"), iri("values"), quad.String(`val2`), nil},
		},
		nil,
	},
	{
		"type shorthand",
		struct {
			rdfType struct{} `quad:"@type > some:Type"`
			item2
			ID     quad.IRI `quad:"@id"`
			Values []string `quad:"values"`
		}{
			item2:  item2{Name: "Sub 1", Spec: "special"},
			ID:     "1234",
			Values: []string{"val1", "val2"},
		},
		iri("1234"),
		[]quad.Quad{
			{iri("1234"), typeIRI, iri("some:Type"), nil},
			{iri("1234"), iri("name"), quad.String("Sub 1"), nil},
			{iri("1234"), iri("spec"), quad.String("special"), nil},
			{iri("1234"), iri("values"), quad.String("val1"), nil},
			{iri("1234"), iri("values"), quad.String("val2"), nil},
		},
		nil,
	},
	{
		"json tags",
		struct {
			rdfType struct{} `quad:"@type > some:Type"`
			item2
			ID     quad.IRI `json:"@id"`
			Values []string `json:"values,omitempty"`
		}{
			item2:  item2{Name: "Sub 1", Spec: "special"},
			ID:     "1234",
			Values: []string{"val1", "val2"},
		},
		iri("1234"),
		[]quad.Quad{
			{iri("1234"), typeIRI, iri("some:Type"), nil},
			{iri("1234"), iri("name"), quad.String("Sub 1"), nil},
			{iri("1234"), iri("spec"), quad.String("special"), nil},
			{iri("1234"), iri("values"), quad.String("val1"), nil},
			{iri("1234"), iri("values"), quad.String("val2"), nil},
		},
		nil,
	},
	{
		"simple object",
		subObject{
			genObject: genObject{
				ID:   "1234",
				Name: "Obj",
			},
			Num: 3,
		},
		iri("1234"),
		[]quad.Quad{
			{iri("1234"), iri("name"), quad.String("Obj"), nil},
			{iri("1234"), iri("num"), quad.Int(3), nil},
		},
		nil,
	},
	{
		"required field not set",
		item2{Name: "partial"},
		nil, nil,
		schema.ErrReqFieldNotSet{Field: "Spec"},
	},
	{
		"single tree node",
		treeItemOpt{
			ID:   iri("n1"),
			Name: "Node 1",
		},
		iri("n1"),
		[]quad.Quad{
			{iri("n1"), iri("name"), quad.String("Node 1"), nil},
		},
		nil,
	},
	{
		"coords",
		Coords{Lat: 12.3, Lng: 34.5},
		nil,
		[]quad.Quad{
			{nil, typeIRI, iri("ex:Coords"), nil},
			{nil, iri("ex:lat"), quad.Float(12.3), nil},
			{nil, iri("ex:lng"), quad.Float(34.5), nil},
		},
		nil,
	},
}

type quadSlice []quad.Quad

func (s *quadSlice) WriteQuad(q quad.Quad) error {
	*s = append(*s, q)
	return nil
}

func TestWriteAsQuads(t *testing.T) {
	for i, c := range testWriteValueCases {
		t.Run(c.name, func(t *testing.T) {
			var out quadSlice
			id, err := schema.WriteAsQuads(&out, c.obj)
			if err != c.err {
				t.Errorf("case %d failed: %v != %v", i, err, c.err)
			} else if c.err != nil {
				return // case with expected error; omit other checks
			}
			if c.id == nil {
				for i := range out {
					if c.expect[i].Subject == nil {
						c.expect[i].Subject = id
					}
				}
			} else if id != c.id {
				t.Errorf("ids are different: %v vs %v", id, c.id)
			}
			if !reflect.DeepEqual([]quad.Quad(out), c.expect) {
				t.Errorf("quad sets are different\n%#v\n%#v", []quad.Quad(out), c.expect)
			}
		})
	}
}

var treeQuads = []quad.Quad{
	{iri("n1"), iri("name"), quad.String("Node 1"), nil},
	{iri("n2"), iri("name"), quad.String("Node 2"), nil},
	{iri("n3"), iri("name"), quad.String("Node 3"), nil},
	{iri("n4"), iri("name"), quad.String("Node 4"), nil},
	{iri("n5"), iri("name"), quad.String("Node 5"), nil},

	{iri("n1"), iri("child"), iri("n2"), nil},
	{iri("n1"), iri("child"), iri("n3"), nil},

	{iri("n3"), iri("child"), iri("n4"), nil},
}

var testFillValueCases = []struct {
	name   string
	expect interface{}
	quads  []quad.Quad
	depth  int
	from   []quad.Value
}{
	{
		name: "complex object",
		expect: struct {
			rdfType struct{} `quad:"rdf:type > some:Type"`
			ID      quad.IRI `quad:"@id"`
			Name    string   `quad:"name"`
			Values  []string `quad:"values"`
			Items   []item   `quad:"items"`
			Sub     *item    `quad:"sub"`
			Val     int      `quad:"val"`
		}{
			ID:     "1234",
			Name:   "some item",
			Values: []string{"val1", "val2"},
			Items: []item{
				{ID: "sub1", Name: "Sub 1"},
				{ID: "sub2", Name: "Sub 2"},
			},
			Sub: &item{ID: "sub3", Name: "Sub 3"},
			Val: 123,
		},
		quads: []quad.Quad{
			{iri("1234"), typeIRI, iri("some:Type"), nil},
			{iri("1234"), iri("name"), quad.String("some item"), nil},
			{iri("1234"), iri("values"), quad.String("val1"), nil},
			{iri("1234"), iri("values"), quad.String("val2"), nil},
			{iri("sub1"), typeIRI, iri("some:item"), nil},
			{iri("sub1"), iri("name"), quad.String("Sub 1"), nil},
			{iri("1234"), iri("items"), iri("sub1"), nil},
			{iri("sub2"), typeIRI, iri("some:item"), nil},
			{iri("sub2"), iri("name"), quad.String("Sub 2"), nil},
			{iri("1234"), iri("items"), iri("sub2"), nil},
			{iri("sub3"), typeIRI, iri("some:item"), nil},
			{iri("sub3"), iri("name"), quad.String("Sub 3"), nil},
			{iri("1234"), iri("sub"), iri("sub3"), nil},
			{iri("1234"), iri("val"), quad.Int(123), nil},
		},
	},
	{
		name: "complex object (id value)",
		expect: struct {
			rdfType struct{}   `quad:"rdf:type > some:Type"`
			ID      quad.Value `quad:"@id"`
			Name    string     `quad:"name"`
			Values  []string   `quad:"values"`
			Items   []item     `quad:"items"`
		}{
			ID:     quad.BNode("1234"),
			Name:   "some item",
			Values: []string{"val1", "val2"},
			Items: []item{
				{ID: "sub1", Name: "Sub 1"},
				{ID: "sub2", Name: "Sub 2"},
			},
		},
		quads: []quad.Quad{
			{quad.BNode("1234"), typeIRI, iri("some:Type"), nil},
			{quad.BNode("1234"), iri("name"), quad.String("some item"), nil},
			{quad.BNode("1234"), iri("values"), quad.String("val1"), nil},
			{quad.BNode("1234"), iri("values"), quad.String("val2"), nil},
			{iri("sub1"), typeIRI, iri("some:item"), nil},
			{iri("sub1"), iri("name"), quad.String("Sub 1"), nil},
			{quad.BNode("1234"), iri("items"), iri("sub1"), nil},
			{iri("sub2"), typeIRI, iri("some:item"), nil},
			{iri("sub2"), iri("name"), quad.String("Sub 2"), nil},
			{quad.BNode("1234"), iri("items"), iri("sub2"), nil},
		},
	},
	{
		name: "embedded object",
		expect: struct {
			rdfType struct{} `quad:"rdf:type > some:Type"`
			item2
			ID     quad.IRI `quad:"@id"`
			Values []string `quad:"values"`
		}{
			item2:  item2{Name: "Sub 1", Spec: "special"},
			ID:     "1234",
			Values: []string{"val1", "val2"},
		},
		quads: []quad.Quad{
			{iri("1234"), typeIRI, iri("some:Type"), nil},
			{iri("1234"), iri("name"), quad.String("Sub 1"), nil},
			{iri("1234"), iri("spec"), quad.String("special"), nil},
			{iri("1234"), iri("values"), quad.String("val1"), nil},
			{iri("1234"), iri("values"), quad.String("val2"), nil},
		},
	},
	{
		name: "type shorthand",
		expect: struct {
			rdfType struct{} `quad:"@type > some:Type"`
			item2
			ID     quad.IRI `quad:"@id"`
			Values []string `quad:"values"`
		}{
			item2:  item2{Name: "Sub 1", Spec: "special"},
			ID:     "1234",
			Values: []string{"val1", "val2"},
		},
		quads: []quad.Quad{
			{iri("1234"), typeIRI, iri("some:Type"), nil},
			{iri("1234"), iri("name"), quad.String("Sub 1"), nil},
			{iri("1234"), iri("spec"), quad.String("special"), nil},
			{iri("1234"), iri("values"), quad.String("val1"), nil},
			{iri("1234"), iri("values"), quad.String("val2"), nil},
		},
	},
	{
		name: "tree",
		expect: treeItem{
			ID:   iri("n1"),
			Name: "Node 1",
			Children: []treeItem{
				{
					ID:   iri("n2"),
					Name: "Node 2",
				},
				{
					ID:   iri("n3"),
					Name: "Node 3",
					Children: []treeItem{
						{
							ID:   iri("n4"),
							Name: "Node 4",
						},
					},
				},
			},
		},
		quads: treeQuads,
		from:  []quad.Value{iri("n1")},
	},
	{
		name: "tree with depth limit 1",
		expect: treeItem{
			ID:   iri("n1"),
			Name: "Node 1",
			Children: []treeItem{
				{
					ID:   iri("n2"),
					Name: "Node 2",
				},
				{
					ID:   iri("n3"),
					Name: "Node 3",
					Children: []treeItem{
						{
							ID: iri("n4"),
						},
					},
				},
			},
		},
		depth: 1,
		quads: treeQuads,
		from:  []quad.Value{iri("n1")},
	},
	{
		name: "tree with depth limit 2",
		expect: treeItemOpt{
			ID:   iri("n1"),
			Name: "Node 1",
			Children: []treeItemOpt{
				{
					ID:   iri("n2"),
					Name: "Node 2",
				},
				{
					ID:   iri("n3"),
					Name: "Node 3",
					Children: []treeItemOpt{
						{
							ID:   iri("n4"),
							Name: "Node 4",
						},
					},
				},
			},
		},
		depth: 2,
		quads: treeQuads,
		from:  []quad.Value{iri("n1")},
	},
	{
		name: "tree with required children",
		expect: treeItemReq{
			ID:   iri("n1"),
			Name: "Node 1",
			Children: []treeItemReq{
				{
					ID:   iri("n3"),
					Name: "Node 3",
					// TODO(dennwc): a strange behavior: this field is required, but it's empty for current object,
					// because all it's children are missing the same field. Leaving this as-is for now because
					// it's weird to set Children field as required in a tree.
					Children: nil,
				},
			},
		},
		quads: treeQuads,
		from:  []quad.Value{iri("n1")},
	},
	{
		name: "simple object",
		expect: subObject{
			genObject: genObject{
				ID:   "1234",
				Name: "Obj",
			},
			Num: 3,
		},
		quads: []quad.Quad{
			{iri("1234"), iri("name"), quad.String("Obj"), nil},
			{iri("1234"), iri("num"), quad.Int(3), nil},
		},
	},
	{
		name:   "coords",
		expect: Coords{Lat: 12.3, Lng: 34.5},
		quads: []quad.Quad{
			{iri("c1"), typeIRI, iri("ex:Coords"), nil},
			{iri("c1"), iri("ex:lat"), quad.Float(12.3), nil},
			{iri("c1"), iri("ex:lng"), quad.Float(34.5), nil},
		},
	},
}

func TestLoadIteratorTo(t *testing.T) {
	for i, c := range testFillValueCases {
		t.Run(c.name, func(t *testing.T) {
			qs := memstore.New(c.quads...)
			out := reflect.New(reflect.TypeOf(c.expect))
			var it graph.Iterator
			if c.from != nil {
				fixed := qs.FixedIterator()
				for _, id := range c.from {
					fixed.Add(qs.ValueOf(id))
				}
				it = fixed
			}
			depth := c.depth
			if depth == 0 {
				depth = -1
			}
			if err := schema.LoadIteratorToDepth(nil, qs, out, depth, it); err != nil {
				t.Errorf("case %d failed: %v", i+1, err)
				return
			}
			got := out.Elem().Interface()
			if s, ok := got.(interface {
				Sort()
			}); ok {
				s.Sort()
			}
			if s, ok := c.expect.(interface {
				Sort()
			}); ok {
				s.Sort()
			}
			if !reflect.DeepEqual(got, c.expect) {
				t.Errorf("case %d failed: objects are different\n%#v\n%#v",
					i+1, out.Elem().Interface(), c.expect,
				)
			}
		})
	}
}

func TestSaveNamespaces(t *testing.T) {
	save := []voc.Namespace{
		{Full: "http://example.org/", Prefix: "ex:"},
		{Full: "http://cayley.io/", Prefix: "c:"},
	}
	var ns voc.Namespaces
	for _, n := range save {
		ns.Register(n)
	}
	qs := memstore.New()
	err := schema.WriteNamespaces(qs, &ns)
	if err != nil {
		t.Fatal(err)
	}
	var ns2 voc.Namespaces
	err = schema.LoadNamespaces(context.TODO(), qs, &ns2)
	if err != nil {
		t.Fatal(err)
	}
	got := ns2.List()
	sort.Sort(voc.ByFullName(save))
	sort.Sort(voc.ByFullName(got))
	if !reflect.DeepEqual(save, got) {
		t.Fatalf("wrong namespaces returned: got: %v, expect: %v", got, save)
	}
	qr := graph.NewQuadStoreReader(qs)
	q, err := quad.ReadAll(qr)
	qr.Close()
	if err != nil {
		t.Fatal(err)
	}
	expect := []quad.Quad{
		quad.MakeIRI("http://cayley.io/", "cayley:prefix", "c:", ""),
		quad.MakeIRI("http://cayley.io/", "rdf:type", "cayley:namespace", ""),

		quad.MakeIRI("http://example.org/", "cayley:prefix", "ex:", ""),
		quad.MakeIRI("http://example.org/", "rdf:type", "cayley:namespace", ""),
	}
	sort.Sort(quad.ByQuadString(expect))
	sort.Sort(quad.ByQuadString(q))
	if !reflect.DeepEqual(expect, q) {
		t.Fatalf("wrong quads returned: got: %v, expect: %v", q, expect)
	}
}
