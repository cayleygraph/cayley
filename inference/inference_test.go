package inference

import (
	"testing"

	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc/rdf"
)

func TestStoreProcessQuad(t *testing.T) {
	store := NewStore()
	q := quad.Quad{Subject: quad.IRI("alice"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI("Person"), Label: nil}
	store.ProcessQuad(q)
	createdClass := store.GetClass(quad.IRI("Person"))
	if createdClass == nil {
		t.Error("Class was not created")
	}
}
