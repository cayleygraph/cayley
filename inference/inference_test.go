package inference

import (
	"testing"

	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc/rdf"
	"github.com/cayleygraph/quad/voc/rdfs"
)

func TestReferencedType(t *testing.T) {
	store := NewStore()
	q := quad.Quad{Subject: quad.IRI("alice"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI("Person"), Label: nil}
	store.ProcessQuad(q)
	createdClass := store.GetClass(quad.IRI("Person"))
	if createdClass == nil {
		t.Error("Class was not created")
	}
}

func TestNewClass(t *testing.T) {
	store := NewStore()
	q := quad.Quad{Subject: quad.IRI("Person"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdfs.Class), Label: nil}
	store.ProcessQuad(q)
	createdClass := store.GetClass(quad.IRI("Person"))
	if createdClass == nil {
		t.Error("Class was not created")
	}
}

func TestNewProperty(t *testing.T) {
	store := NewStore()
	q := quad.Quad{Subject: quad.IRI("name"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdf.Property), Label: nil}
	store.ProcessQuad(q)
	createdProperty := store.GetProperty(quad.IRI("name"))
	if createdProperty == nil {
		t.Error("Property was not created")
	}
}
