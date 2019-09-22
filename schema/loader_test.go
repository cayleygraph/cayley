package schema_test

import (
	"reflect"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/cayley/schema"
	"github.com/cayleygraph/quad"
)

func TestLoadLoop(t *testing.T) {
	sch := schema.NewConfig()

	a := &NodeLoop{ID: iri("A"), Name: "Node A"}
	a.Next = a

	qs := memstore.New([]quad.Quad{
		{a.ID, iri("name"), quad.String(a.Name), nil},
		{a.ID, iri("next"), a.ID, nil},
	}...)

	b := &NodeLoop{}
	if err := sch.LoadIteratorTo(nil, qs, reflect.ValueOf(b), nil); err != nil {
		t.Error(err)
		return
	}
	if a.ID != b.ID || a.Name != b.Name {
		t.Fatalf("%#v vs %#v", a, b)
	}
	if b != b.Next {
		t.Fatalf("loop is broken: %p vs %p", b, b.Next)
	}

	a = &NodeLoop{ID: iri("A"), Name: "Node A"}
	b = &NodeLoop{ID: iri("B"), Name: "Node B"}
	c := &NodeLoop{ID: iri("C"), Name: "Node C"}
	a.Next = b
	b.Next = c
	c.Next = a

	qs = memstore.New([]quad.Quad{
		{a.ID, iri("name"), quad.String(a.Name), nil},
		{b.ID, iri("name"), quad.String(b.Name), nil},
		{c.ID, iri("name"), quad.String(c.Name), nil},
		{a.ID, iri("next"), b.ID, nil},
		{b.ID, iri("next"), c.ID, nil},
		{c.ID, iri("next"), a.ID, nil},
	}...)

	a1 := &NodeLoop{}
	if err := sch.LoadIteratorTo(nil, qs, reflect.ValueOf(a1), nil); err != nil {
		t.Error(err)
		return
	}
	if a.ID != a1.ID || a.Name != a1.Name {
		t.Fatalf("%#v vs %#v", a, b)
	}
	b1 := a1.Next
	c1 := b1.Next
	if b.ID != b1.ID || b.Name != b1.Name {
		t.Fatalf("%#v vs %#v", a, b)
	}
	if c.ID != c1.ID || c.Name != c1.Name {
		t.Fatalf("%#v vs %#v", a, b)
	}
	if a1 != c1.Next {
		t.Fatalf("loop is broken: %p vs %p", a1, c1.Next)
	}
}

func TestLoadIteratorTo(t *testing.T) {
	sch := schema.NewConfig()
	for i, c := range testFillValueCases {
		t.Run(c.name, func(t *testing.T) {
			qs := memstore.New(c.quads...)
			rt := reflect.TypeOf(c.expect)
			var out reflect.Value
			if rt.Kind() == reflect.Ptr {
				out = reflect.New(rt.Elem())
			} else {
				out = reflect.New(rt)
			}
			var it graph.Iterator
			if c.from != nil {
				fixed := iterator.NewFixed()
				for _, id := range c.from {
					fixed.Add(qs.ValueOf(id))
				}
				it = fixed
			}
			depth := c.depth
			if depth == 0 {
				depth = -1
			}
			if err := sch.LoadIteratorToDepth(nil, qs, out, depth, it); err != nil {
				t.Errorf("case %d failed: %v", i+1, err)
				return
			}
			var got interface{}
			if rt.Kind() == reflect.Ptr {
				got = out.Interface()
			} else {
				got = out.Elem().Interface()
			}
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
					i+1, got, c.expect,
				)
			}
		})
	}
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
		name: "typedef",
		expect: genObjectTypedef{
			ID:   "1234",
			Name: "Obj",
		},
		quads: []quad.Quad{
			{iri("1234"), iri("name"), quad.String("Obj"), nil},
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
	{
		name: "same node",
		expect: NestedNode{
			ID:   "c1",
			Name: "A",
			Prev: genObject{
				ID:   "c2",
				Name: "B",
			},
			Next: genObject{
				ID:   "c2",
				Name: "B",
			},
		},
		quads: []quad.Quad{
			{iri("c1"), iri("name"), quad.String("A"), nil},
			{iri("c2"), iri("name"), quad.String("B"), nil},
			{iri("c1"), iri("next"), iri("c2"), nil},
			{iri("c1"), iri("prev"), iri("c2"), nil},
		},
	},
	{
		name: "all optional",
		expect: Alts{
			Alt: []OptFields{
				{One: "A"},
				{Two: "B"},
				{One: "C", Two: "D"},
			},
		},
		quads: []quad.Quad{
			{iri("c1"), iri("alt"), iri("h1"), nil},
			{iri("c1"), iri("alt"), iri("h2"), nil},
			{iri("c1"), iri("alt"), iri("h3"), nil},

			{iri("h1"), iri("one"), quad.String("A"), nil},
			{iri("h2"), iri("two"), quad.String("B"), nil},
			{iri("h3"), iri("one"), quad.String("C"), nil},
			{iri("h3"), iri("two"), quad.String("D"), nil},
		},
	},
}
