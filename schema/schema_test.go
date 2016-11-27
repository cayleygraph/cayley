package schema_test

import (
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

func iri(s string) quad.IRI { return quad.IRI(s) }

const typeIRI = quad.IRI(rdf.Type)

var testWriteValueCases = []struct {
	obj    interface{}
	id     quad.Value
	expect []quad.Quad
}{
	{
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
	},
	{
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
	},
	{
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
	},
	{
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
	},
}

type quadSlice []quad.Quad

func (s *quadSlice) WriteQuad(q quad.Quad) error {
	*s = append(*s, q)
	return nil
}

func TestWriteAsQuads(t *testing.T) {
	for i, c := range testWriteValueCases {
		var out quadSlice
		if id, err := schema.WriteAsQuads(&out, c.obj); err != nil {
			t.Errorf("case %d failed: %v", i, err)
		} else if id != c.id {
			t.Errorf("ids are different: %v vs %v", id, c.id)
		} else if !reflect.DeepEqual([]quad.Quad(out), c.expect) {
			t.Errorf("quad sets are different\n%#v\n%#v", []quad.Quad(out), c.expect)
		}
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
	expect interface{}
	quads  []quad.Quad
	from   []quad.Value
}{
	{
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
		quads: treeQuads,
		from:  []quad.Value{iri("n1")},
	},
	{
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
}

func TestSaveIteratorTo(t *testing.T) {
	for i, c := range testFillValueCases {
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
		if err := schema.LoadIteratorTo(nil, qs, out, it); err != nil {
			t.Errorf("case %d failed: %v", i+1, err)
			continue
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
	err = schema.LoadNamespaces(qs, &ns2)
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
