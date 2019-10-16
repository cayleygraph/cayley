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

func TestSubClass(t *testing.T) {
	store := NewStore()
	q := quad.Quad{Subject: quad.IRI("Engineer"), Predicate: quad.IRI(rdfs.SubClassOf), Object: quad.IRI("Person"), Label: nil}
	store.ProcessQuad(q)
	createdClass := store.GetClass(quad.IRI("Engineer"))
	createdSuperClass := store.GetClass(quad.IRI("Person"))
	if createdClass == nil {
		t.Error("Class was not created")
	}
	if createdSuperClass == nil {
		t.Error("Super class was not created")
	}
	if _, ok := createdClass.super[createdSuperClass]; !ok {
		t.Error("Super class was not registered for class")
	}
	if _, ok := createdSuperClass.sub[createdClass]; !ok {
		t.Error("Class was not registered for super class")
	}
}

func TestSubProperty(t *testing.T) {
	store := NewStore()
	q := quad.Quad{Subject: quad.IRI("name"), Predicate: quad.IRI(rdfs.SubPropertyOf), Object: quad.IRI("personal"), Label: nil}
	store.ProcessQuad(q)
	createdProperty := store.GetProperty(quad.IRI("name"))
	createdSuperProperty := store.GetProperty(quad.IRI("personal"))
	if createdProperty == nil {
		t.Error("Property was not created")
	}
	if createdSuperProperty == nil {
		t.Error("Super property was not created")
	}
	if _, ok := createdProperty.super[createdSuperProperty]; !ok {
		t.Error("Super property was not registered for property")
	}
	if _, ok := createdSuperProperty.sub[createdProperty]; !ok {
		t.Error("Property was not registered for super property")
	}
}

func TestPropertyDomain(t *testing.T) {
	store := NewStore()
	q := quad.Quad{Subject: quad.IRI("name"), Predicate: quad.IRI(rdfs.Domain), Object: quad.IRI("Person"), Label: nil}
	store.ProcessQuad(q)
	createdProperty := store.GetProperty(quad.IRI("name"))
	createdClass := store.GetClass(quad.IRI("Person"))
	if createdProperty == nil {
		t.Error("Property was not created")
	}
	if createdClass == nil {
		t.Error("Domain class was not created")
	}
	if createdProperty.Domain() != createdClass {
		t.Error("Domain class was not registered for property")
	}
}

func TestPropertyRange(t *testing.T) {
	store := NewStore()
	q := quad.Quad{Subject: quad.IRI("name"), Predicate: quad.IRI(rdfs.Range), Object: quad.IRI("Person"), Label: nil}
	store.ProcessQuad(q)
	createdProperty := store.GetProperty(quad.IRI("name"))
	createdClass := store.GetClass(quad.IRI("Person"))
	if createdProperty == nil {
		t.Error("Property was not created")
	}
	if createdClass == nil {
		t.Error("Range class was not created")
	}
	if createdProperty.Range() != createdClass {
		t.Error("Range class was not registered for property")
	}
}
