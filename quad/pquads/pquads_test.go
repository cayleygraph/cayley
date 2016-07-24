package pquads_test

import (
	"bytes"
	"testing"

	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/quad/pquads"
	"reflect"
)

var testData = []struct {
	quads []quad.Quad
}{
	{
		[]quad.Quad{
			{
				Subject:   quad.BNode("subject1"),
				Predicate: quad.IRI("/film/performance/character"),
				Object:    quad.String("Tomás de Torquemada"),
				Label:     nil,
			},
			{
				Subject:   quad.BNode("subject1"),
				Predicate: quad.IRI("http://an.example/predicate1"),
				Object:    quad.String("object1"),
				Label:     nil,
			},
			{
				Subject:   quad.IRI("http://example.org/bob#me"),
				Predicate: quad.IRI("http://schema.org/birthDate"),
				Object: quad.TypedString{
					Value: "1990-07-04",
					Type:  "http://www.w3.org/2001/XMLSchema#date",
				},
				Label: nil,
			},
		},
	},
}

func TestWriter(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	for _, c := range testData {
		buf.Reset()
		w := pquads.NewWriter(buf, true)
		n, err := quad.Copy(w, quad.NewReader(c.quads))
		if err != nil {
			t.Fatalf("write failed after %d quads: %v", n, err)
		}
		if err = w.Close(); err != nil {
			t.Fatal("error on close:", err)
		}
		r := pquads.NewReader(buf, 0)
		quads, err := quad.ReadAll(r)
		if err != nil {
			t.Fatalf("read failed: %v", err)
		}
		if err = r.Close(); err != nil {
			t.Fatal("error on close:", err)
		}
		if !reflect.DeepEqual(c.quads, quads) {
			t.Fatal("corrupted data")
		}
	}
}
