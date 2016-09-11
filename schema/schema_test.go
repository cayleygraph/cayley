package schema_test

import (
	"reflect"
	"testing"

	"github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/schema"
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
		quad.IRI("1234"),
		[]quad.Quad{
			{quad.IRI("1234"), typeIRI, quad.IRI("some:Type"), nil},
			{quad.IRI("1234"), quad.IRI("name"), quad.String(`some item`), nil},
			{quad.IRI("1234"), quad.IRI("values"), quad.String(`val1`), nil},
			{quad.IRI("1234"), quad.IRI("values"), quad.String(`val2`), nil},

			{quad.IRI("sub1"), typeIRI, quad.IRI("some:item"), nil},
			{quad.IRI("sub1"), quad.IRI("name"), quad.String(`Sub 1`), nil},
			{quad.IRI("1234"), quad.IRI("items"), quad.IRI("sub1"), nil},

			{quad.IRI("sub2"), typeIRI, quad.IRI("some:item"), nil},
			{quad.IRI("sub2"), quad.IRI("name"), quad.String(`Sub 2`), nil},
			{quad.IRI("1234"), quad.IRI("items"), quad.IRI("sub2"), nil},

			{quad.IRI("sub3"), typeIRI, quad.IRI("some:item"), nil},
			{quad.IRI("sub3"), quad.IRI("name"), quad.String(`Sub 3`), nil},
			{quad.IRI("1234"), quad.IRI("sub"), quad.IRI("sub3"), nil},
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
		quad.IRI("1234"),
		[]quad.Quad{
			{quad.IRI("1234"), typeIRI, quad.IRI("some:Type"), nil},
			{quad.IRI("1234"), quad.IRI("name"), quad.String(`Sub 1`), nil},
			{quad.IRI("1234"), quad.IRI("spec"), quad.String(`special`), nil},
			{quad.IRI("1234"), quad.IRI("values"), quad.String(`val1`), nil},
			{quad.IRI("1234"), quad.IRI("values"), quad.String(`val2`), nil},
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
		quad.IRI("1234"),
		[]quad.Quad{
			{quad.IRI("1234"), typeIRI, quad.IRI("some:Type"), nil},
			{quad.IRI("1234"), quad.IRI("name"), quad.String("Sub 1"), nil},
			{quad.IRI("1234"), quad.IRI("spec"), quad.String("special"), nil},
			{quad.IRI("1234"), quad.IRI("values"), quad.String("val1"), nil},
			{quad.IRI("1234"), quad.IRI("values"), quad.String("val2"), nil},
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
		quad.IRI("1234"),
		[]quad.Quad{
			{quad.IRI("1234"), typeIRI, quad.IRI("some:Type"), nil},
			{quad.IRI("1234"), quad.IRI("name"), quad.String("Sub 1"), nil},
			{quad.IRI("1234"), quad.IRI("spec"), quad.String("special"), nil},
			{quad.IRI("1234"), quad.IRI("values"), quad.String("val1"), nil},
			{quad.IRI("1234"), quad.IRI("values"), quad.String("val2"), nil},
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

var testFillValueCases = []struct {
	expect interface{}
	quads  []quad.Quad
}{
	{
		struct {
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
		[]quad.Quad{
			{quad.IRI("1234"), typeIRI, quad.IRI("some:Type"), nil},
			{quad.IRI("1234"), quad.IRI("name"), quad.String("some item"), nil},
			{quad.IRI("1234"), quad.IRI("values"), quad.String("val1"), nil},
			{quad.IRI("1234"), quad.IRI("values"), quad.String("val2"), nil},
			{quad.IRI("sub1"), typeIRI, quad.IRI("some:item"), nil},
			{quad.IRI("sub1"), quad.IRI("name"), quad.String("Sub 1"), nil},
			{quad.IRI("1234"), quad.IRI("items"), quad.IRI("sub1"), nil},
			{quad.IRI("sub2"), typeIRI, quad.IRI("some:item"), nil},
			{quad.IRI("sub2"), quad.IRI("name"), quad.String("Sub 2"), nil},
			{quad.IRI("1234"), quad.IRI("items"), quad.IRI("sub2"), nil},
			{quad.IRI("sub3"), typeIRI, quad.IRI("some:item"), nil},
			{quad.IRI("sub3"), quad.IRI("name"), quad.String("Sub 3"), nil},
			{quad.IRI("1234"), quad.IRI("sub"), quad.IRI("sub3"), nil},
			{quad.IRI("1234"), quad.IRI("val"), quad.Int(123), nil},
		},
	},
	{
		struct {
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
		[]quad.Quad{
			{quad.BNode("1234"), typeIRI, quad.IRI("some:Type"), nil},
			{quad.BNode("1234"), quad.IRI("name"), quad.String("some item"), nil},
			{quad.BNode("1234"), quad.IRI("values"), quad.String("val1"), nil},
			{quad.BNode("1234"), quad.IRI("values"), quad.String("val2"), nil},
			{quad.IRI("sub1"), typeIRI, quad.IRI("some:item"), nil},
			{quad.IRI("sub1"), quad.IRI("name"), quad.String("Sub 1"), nil},
			{quad.BNode("1234"), quad.IRI("items"), quad.IRI("sub1"), nil},
			{quad.IRI("sub2"), typeIRI, quad.IRI("some:item"), nil},
			{quad.IRI("sub2"), quad.IRI("name"), quad.String("Sub 2"), nil},
			{quad.BNode("1234"), quad.IRI("items"), quad.IRI("sub2"), nil},
		},
	},
	//	{
	//			struct {
	//			rdfType struct{} `quad:"rdf:type > some:Type"`
	//			item2
	//			ID     quad.IRI `quad:"."`
	//			Values []string `quad:"values"`
	//		}{
	//			item2:  item2{Name: "Sub 1", Spec: "special"},
	//			ID:     "1234",
	//			Values: []string{"val1", "val2"},
	//		},
	//		[]quad.Quad{
	//			{quad.IRI("1234"), typeIRI, quad.IRI("some:Type"), nil},
	//			{quad.IRI("1234"), quad.IRI("name"), quad.String("Sub 1"), nil},
	//			{quad.IRI("1234"), quad.IRI("spec"), quad.String("special"), nil},
	//			{quad.IRI("1234"), quad.IRI("values"), quad.String("val1"), nil},
	//			{quad.IRI("1234"), quad.IRI("values"), quad.String("val2"), nil},
	//		},
	//	},
	//	{
	//			struct {
	//			rdfType struct{} `quad:"@type > some:Type"`
	//			item2
	//			ID     quad.IRI `quad:"@id"`
	//			Values []string `quad:"values"`
	//		}{
	//			item2:  item2{Name: "Sub 1", Spec: "special"},
	//			ID:     "1234",
	//			Values: []string{"val1", "val2"},
	//		},
	//		[]quad.Quad{
	//			{quad.IRI("1234"), typeIRI, quad.IRI("some:Type"), nil},
	//			{quad.IRI("1234"), quad.IRI("name"), quad.String("Sub 1"), nil},
	//			{quad.IRI("1234"), quad.IRI("spec"), quad.String("special"), nil},
	//			{quad.IRI("1234"), quad.IRI("values"), quad.String("val1"), nil},
	//			{quad.IRI("1234"), quad.IRI("values"), quad.String("val2"), nil},
	//		},
	//	},
}

func TestSaveIteratorTo(t *testing.T) {
	for i, c := range testFillValueCases {
		qs := memstore.New(c.quads...)
		out := reflect.New(reflect.TypeOf(c.expect))
		if err := schema.SaveIteratorTo(nil, qs, out, nil); err != nil {
			t.Errorf("case %d failed: %v", i+1, err)
		} else if !reflect.DeepEqual(out.Elem().Interface(), c.expect) {
			t.Errorf("case %d failed: objects are different\n%#v\n%#v",
				i+1, out.Elem().Interface(), c.expect,
			)
		}
	}
}
