package schema_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/cayleygraph/cayley/schema"
	"github.com/cayleygraph/quad"
)

type quadSlice []quad.Quad

func (s *quadSlice) WriteQuad(q quad.Quad) error {
	*s = append(*s, q)
	return nil
}

func (s *quadSlice) WriteQuads(buf []quad.Quad) (int, error) {
	*s = append(*s, buf...)
	return len(buf), nil
}

func TestWriteAsQuads(t *testing.T) {
	sch := schema.NewConfig()
	for _, c := range testWriteValueCases {
		t.Run(c.name, func(t *testing.T) {
			var out quadSlice
			id, err := sch.WriteAsQuads(&out, c.obj)
			if err != c.err {
				t.Errorf("unexpected error: %v (expected: %v)", err, c.err)
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
		"typedef",
		genObjectTypedef{
			ID:   "1234",
			Name: "Obj",
		},
		iri("1234"),
		[]quad.Quad{
			{iri("1234"), iri("name"), quad.String("Obj"), nil},
		},
		nil,
	},
	{
		"simple object (embedded multiple levels)",
		subSubObject{
			subObject: subObject{
				genObject: genObject{
					ID:   "1234",
					Name: "Obj",
				},
				Num: 3,
			},
			Num2: 4,
		},
		iri("1234"),
		[]quad.Quad{
			{iri("1234"), iri("name"), quad.String("Obj"), nil},
			{iri("1234"), iri("num"), quad.Int(3), nil},
			{iri("1234"), iri("num2"), quad.Int(4), nil},
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
		"required unexported",
		timeItem{ID: "1", Name: "t", TS: time.Unix(100, 0)},
		nil,
		[]quad.Quad{
			{iri("1"), iri("name"), quad.String("t"), nil},
			{iri("1"), iri("ts"), quad.Time(time.Unix(100, 0)), nil},
		},
		nil,
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
		"nested tree nodes",
		treeItemOpt{
			ID:   iri("n1"),
			Name: "Node 1",
			Children: []treeItemOpt{
				{ID: iri("n2"), Name: "Node 2"},
			},
		},
		iri("n1"),
		[]quad.Quad{
			{iri("n1"), iri("name"), quad.String("Node 1"), nil},
			{iri("n2"), iri("name"), quad.String("Node 2"), nil},
			{iri("n1"), iri("child"), iri("n2"), nil},
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
	{
		"self loop",
		func() *NodeLoop {
			a := &NodeLoop{ID: iri("A"), Name: "Node A"}
			a.Next = a
			return a
		}(),
		iri("A"),
		[]quad.Quad{
			{iri("A"), iri("name"), quad.String("Node A"), nil},
			{iri("A"), iri("next"), iri("A"), nil},
		},
		nil,
	},
	{
		"pointer chain",
		func() *NodeLoop {
			a := &NodeLoop{ID: iri("A"), Name: "Node A"}
			b := &NodeLoop{ID: iri("B"), Name: "Node B"}
			c := &NodeLoop{ID: iri("C"), Name: "Node C"}

			a.Next = b
			b.Next = c
			c.Next = a
			return a
		}(),
		iri("A"),
		[]quad.Quad{
			{iri("A"), iri("name"), quad.String("Node A"), nil},
			{iri("B"), iri("name"), quad.String("Node B"), nil},
			{iri("C"), iri("name"), quad.String("Node C"), nil},
			{iri("C"), iri("next"), iri("A"), nil},
			{iri("B"), iri("next"), iri("C"), nil},
			{iri("A"), iri("next"), iri("B"), nil},
		},
		nil,
	},
}
