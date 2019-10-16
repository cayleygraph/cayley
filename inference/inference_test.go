package inference

import (
	"testing"

	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc/rdf"
	"github.com/cayleygraph/quad/voc/rdfs"
)

func TestClassName(t *testing.T) {
	iri := quad.IRI("alice")
	c := Class{name: iri}
	if c.Name() != iri {
		t.Error("Name was not set correctly for the class")
	}
}

func TestPropertyName(t *testing.T) {
	iri := quad.IRI("likes")
	p := Property{name: iri}
	if p.Name() != iri {
		t.Error("Name was not set correctly for the class")
	}
}

func TestReferencedType(t *testing.T) {
	store := NewStore()
	q := quad.Quad{Subject: quad.IRI("alice"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI("Person"), Label: nil}
	store.ProcessQuad(q)
	createdClass := store.GetClass(quad.IRI("Person"))
	if createdClass == nil {
		t.Error("Class was not created")
	}
}

func TestReferencedBNodeType(t *testing.T) {
	store := NewStore()
	name := quad.BNode("123")
	q := quad.Quad{Subject: quad.IRI("alice"), Predicate: quad.IRI(rdf.Type), Object: name, Label: nil}
	store.ProcessQuad(q)
	createdClass := store.GetClass(name)
	if createdClass == nil {
		t.Error("Class was not created")
	}
}

func TestReferencedProperty(t *testing.T) {
	store := NewStore()
	q := quad.Quad{Subject: quad.IRI("alice"), Predicate: quad.IRI("likes"), Object: quad.IRI("bob"), Label: nil}
	store.ProcessQuad(q)
	createdProperty := store.GetProperty(quad.IRI("likes"))
	if createdProperty == nil {
		t.Error("Property was not created")
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

func TestNewBNodeClass(t *testing.T) {
	store := NewStore()
	name := quad.BNode("123")
	q := quad.Quad{Subject: name, Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdfs.Class), Label: nil}
	store.ProcessQuad(q)
	createdClass := store.GetClass(name)
	if createdClass == nil {
		t.Error("Class was not created")
	}
}

func TestInvalidNewClass(t *testing.T) {
	store := NewStore()
	name := quad.String("Foo")
	q := quad.Quad{Subject: quad.IRI("alice"), Predicate: quad.IRI(rdf.Type), Object: name, Label: nil}
	store.ProcessQuad(q)
	createdClass := store.GetClass(name)
	if createdClass != nil {
		t.Error("Invalid class was created")
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

func TestInvalidNewProperty(t *testing.T) {
	store := NewStore()
	name := quad.String("Foo")
	q := quad.Quad{Subject: quad.IRI("alice"), Predicate: name, Object: quad.IRI("bob"), Label: nil}
	store.ProcessQuad(q)
	createdProperty := store.GetProperty(name)
	if createdProperty != nil {
		t.Error("Invalid property was created")
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
	if _, ok := createdClass.ownProperties[createdProperty]; !ok {
		t.Error("Property was not registered for class")
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
	if _, ok := createdClass.inProperties[createdProperty]; !ok {
		t.Error("Property was not registered for class")
	}
}

func TestIsSubClassOf(t *testing.T) {
	store := NewStore()
	q := quad.Quad{Subject: quad.IRI("Engineer"), Predicate: quad.IRI(rdfs.SubClassOf), Object: quad.IRI("Person")}
	store.ProcessQuad(q)
	if !store.GetClass(quad.IRI("Engineer")).IsSubClassOf(store.GetClass(quad.IRI("Person"))) {
		t.Error("Class was not registered as subclass of super class")
	}
}

func TestIsSubClassOfRecursive(t *testing.T) {
	store := NewStore()
	quads := []quad.Quad{
		quad.Quad{Subject: quad.IRI("Engineer"), Predicate: quad.IRI(rdfs.SubClassOf), Object: quad.IRI("Person")},
		quad.Quad{Subject: quad.IRI("SoftwareEngineer"), Predicate: quad.IRI(rdfs.SubClassOf), Object: quad.IRI("Engineer")},
	}
	store.ProcessQuads(quads)
	if !store.GetClass(quad.IRI("SoftwareEngineer")).IsSubClassOf(store.GetClass(quad.IRI("Person"))) {
		t.Error("Class was not registered as subclass of super class")
	}
}

func TestIsSubPropertyOf(t *testing.T) {
	store := NewStore()
	q := quad.Quad{Subject: quad.IRI("name"), Predicate: quad.IRI(rdfs.SubPropertyOf), Object: quad.IRI("personal"), Label: nil}
	store.ProcessQuad(q)
	if !store.GetProperty(quad.IRI("name")).IsSubPropertyOf(store.GetProperty(quad.IRI("personal"))) {
		t.Error("Property was not registered as subproperty of super property")
	}
}

func TestIsSubPropertyOfRecursive(t *testing.T) {
	store := NewStore()
	quads := []quad.Quad{
		quad.Quad{Subject: quad.IRI("name"), Predicate: quad.IRI(rdfs.SubPropertyOf), Object: quad.IRI("personal"), Label: nil},
		quad.Quad{Subject: quad.IRI("personal"), Predicate: quad.IRI(rdfs.SubPropertyOf), Object: quad.IRI("information"), Label: nil},
	}
	store.ProcessQuads(quads)
	if !store.GetProperty(quad.IRI("name")).IsSubPropertyOf(store.GetProperty(quad.IRI("information"))) {
		t.Error("Property was not registered as subproperty of super property")
	}
}

func TestDeleteReferencedType(t *testing.T) {
	store := NewStore()
	q := quad.Quad{Subject: quad.IRI("alice"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI("Person"), Label: nil}
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	createdClass := store.GetClass(quad.IRI("Person"))
	if createdClass != nil {
		t.Error("Class was not deleted")
	}
}

func TestDeleteClassWithSubClass(t *testing.T) {
	store := NewStore()
	store.ProcessQuads([]quad.Quad{
		quad.Quad{Subject: quad.IRI("Engineer"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdfs.Class), Label: nil},
		quad.Quad{Subject: quad.IRI("Engineer"), Predicate: quad.IRI(rdfs.SubClassOf), Object: quad.IRI("Person"), Label: nil},
	})
	q := quad.Quad{Subject: quad.IRI("Person"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdfs.Class), Label: nil}
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	subClass := store.GetClass(quad.IRI("Engineer"))
	if len(subClass.super) != 0 {
		t.Error("Class was not unreferenced")
	}
}

func TestDeleteClassWithSuperClass(t *testing.T) {
	store := NewStore()
	store.ProcessQuads([]quad.Quad{
		quad.Quad{Subject: quad.IRI("Person"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdfs.Class), Label: nil},
		quad.Quad{Subject: quad.IRI("Engineer"), Predicate: quad.IRI(rdfs.SubClassOf), Object: quad.IRI("Person"), Label: nil},
	})
	q := quad.Quad{Subject: quad.IRI("Engineer"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdfs.Class), Label: nil}
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	superClass := store.GetClass(quad.IRI("Person"))
	if len(superClass.sub) != 0 {
		t.Error("Class was not unreferenced")
	}
}

func TestDeleteNewClass(t *testing.T) {
	store := NewStore()
	q := quad.Quad{Subject: quad.IRI("Person"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdfs.Class), Label: nil}
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	createdClass := store.GetClass(quad.IRI("Person"))
	if createdClass != nil {
		t.Error("Class was not deleted")
	}
}

func TestDeleteNewProperty(t *testing.T) {
	store := NewStore()
	q := quad.Quad{Subject: quad.IRI("name"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdf.Property), Label: nil}
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	createdProperty := store.GetProperty(quad.IRI("name"))
	if createdProperty != nil {
		t.Error("Property was not deleted")
	}
}

func TestDeletePropertyWithSubProperty(t *testing.T) {
	store := NewStore()
	store.ProcessQuads([]quad.Quad{
		quad.Quad{Subject: quad.IRI("name"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdf.Property), Label: nil},
		quad.Quad{Subject: quad.IRI("name"), Predicate: quad.IRI(rdfs.SubPropertyOf), Object: quad.IRI("personal"), Label: nil},
	})
	q := quad.Quad{Subject: quad.IRI("personal"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdf.Property), Label: nil}
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	subProperty := store.GetProperty(quad.IRI("name"))
	if len(subProperty.super) != 0 {
		t.Error("Property was not unreferenced")
	}
}

func TestDeletePropertyWithSuperProperty(t *testing.T) {
	store := NewStore()
	store.ProcessQuads([]quad.Quad{
		quad.Quad{Subject: quad.IRI("personal"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdf.Property), Label: nil},
		quad.Quad{Subject: quad.IRI("name"), Predicate: quad.IRI(rdfs.SubPropertyOf), Object: quad.IRI("personal"), Label: nil},
	})
	q := quad.Quad{Subject: quad.IRI("name"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdf.Property), Label: nil}
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	superProperty := store.GetProperty(quad.IRI("personal"))
	if len(superProperty.sub) != 0 {
		t.Error("Property was not unreferenced")
	}
}

func TestDeleteSubClass(t *testing.T) {
	store := NewStore()
	store.ProcessQuads([]quad.Quad{
		quad.Quad{Subject: quad.IRI("Engineer"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdfs.Class), Label: nil},
		quad.Quad{Subject: quad.IRI("Person"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdfs.Class), Label: nil},
	})
	q := quad.Quad{Subject: quad.IRI("Engineer"), Predicate: quad.IRI(rdfs.SubClassOf), Object: quad.IRI("Person"), Label: nil}
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	createdClass := store.GetClass(quad.IRI("Engineer"))
	createdSuperClass := store.GetClass(quad.IRI("Person"))
	// TODO(iddan): what about garbage collection?
	if _, ok := createdClass.super[createdSuperClass]; ok {
		t.Error("Super class was not unregistered for class")
	}
	if _, ok := createdSuperClass.sub[createdClass]; ok {
		t.Error("Class was not unregistered for super class")
	}
}

func TestDeleteSubProperty(t *testing.T) {
	store := NewStore()
	store.ProcessQuads([]quad.Quad{
		quad.Quad{Subject: quad.IRI("name"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdf.Property), Label: nil},
		quad.Quad{Subject: quad.IRI("personal"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdf.Property), Label: nil},
	})
	q := quad.Quad{Subject: quad.IRI("name"), Predicate: quad.IRI(rdfs.SubPropertyOf), Object: quad.IRI("personal"), Label: nil}
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	createdProperty := store.GetProperty(quad.IRI("name"))
	createdSuperProperty := store.GetProperty(quad.IRI("personal"))
	// TODO(iddan): what about garbage collection?
	if _, ok := createdProperty.super[createdSuperProperty]; ok {
		t.Error("Super property was not unregistered for property")
	}
	if _, ok := createdSuperProperty.sub[createdProperty]; ok {
		t.Error("Property was not unregistered for super property")
	}
}

func TestDeletePropertyDomain(t *testing.T) {
	store := NewStore()
	store.ProcessQuads([]quad.Quad{
		quad.Quad{Subject: quad.IRI("name"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdf.Property), Label: nil},
		quad.Quad{Subject: quad.IRI("Person"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdfs.Class), Label: nil},
	})
	q := quad.Quad{Subject: quad.IRI("name"), Predicate: quad.IRI(rdfs.Domain), Object: quad.IRI("Person"), Label: nil}
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	createdProperty := store.GetProperty(quad.IRI("name"))
	createdClass := store.GetClass(quad.IRI("Person"))
	// TODO(iddan): what about garbage collection?
	if createdProperty.Domain() == createdClass {
		t.Error("Domain class was not unregistered for property")
	}
	if _, ok := createdClass.ownProperties[createdProperty]; ok {
		t.Error("Property was not unregistered for class")
	}
}

func TestDeletePropertyRange(t *testing.T) {
	store := NewStore()
	store.ProcessQuads([]quad.Quad{
		quad.Quad{Subject: quad.IRI("name"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdf.Property), Label: nil},
		quad.Quad{Subject: quad.IRI(rdfs.Literal), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdfs.Class), Label: nil},
	})
	q := quad.Quad{Subject: quad.IRI("name"), Predicate: quad.IRI(rdfs.Range), Object: quad.IRI(rdfs.Literal), Label: nil}
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	createdProperty := store.GetProperty(quad.IRI("name"))
	createdClass := store.GetClass(quad.IRI(rdfs.Literal))
	// TODO(iddan): what about garbage collection?
	if createdProperty.Range() == createdClass {
		t.Error("Range class was not unregistered for property")
	}
	if _, ok := createdClass.inProperties[createdProperty]; ok {
		t.Error("Property was not unregistered for class")
	}
}

func TestDeleteIsSubClassOf(t *testing.T) {
	store := NewStore()
	store.ProcessQuads([]quad.Quad{
		quad.Quad{Subject: quad.IRI("Engineer"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdfs.Class), Label: nil},
		quad.Quad{Subject: quad.IRI("Person"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdfs.Class), Label: nil},
	})
	q := quad.Quad{Subject: quad.IRI("Engineer"), Predicate: quad.IRI(rdfs.SubClassOf), Object: quad.IRI("Person")}
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	if store.GetClass(quad.IRI("Engineer")).IsSubClassOf(store.GetClass(quad.IRI("Person"))) {
		t.Error("Class was not unregistered as subclass of super class")
	}
}

func TestDeleteIsSubClassOfRecursive(t *testing.T) {
	store := NewStore()
	store.ProcessQuads([]quad.Quad{
		quad.Quad{Subject: quad.IRI("Engineer"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdfs.Class), Label: nil},
		quad.Quad{Subject: quad.IRI("Person"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdfs.Class), Label: nil},
		quad.Quad{Subject: quad.IRI("SoftwareEngineer"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdfs.Class), Label: nil},
	})
	quads := []quad.Quad{
		quad.Quad{Subject: quad.IRI("Engineer"), Predicate: quad.IRI(rdfs.SubClassOf), Object: quad.IRI("Person")},
		quad.Quad{Subject: quad.IRI("SoftwareEngineer"), Predicate: quad.IRI(rdfs.SubClassOf), Object: quad.IRI("Engineer")},
	}
	store.ProcessQuads(quads)
	store.UnprocessQuads(quads)
	if store.GetClass(quad.IRI("SoftwareEngineer")).IsSubClassOf(store.GetClass(quad.IRI("Person"))) {
		t.Error("Class was not unregistered as subclass of super class")
	}
}

func TestDeleteIsSubPropertyOf(t *testing.T) {
	store := NewStore()
	store.ProcessQuads([]quad.Quad{
		quad.Quad{Subject: quad.IRI("name"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdf.Property), Label: nil},
		quad.Quad{Subject: quad.IRI("personal"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdf.Property), Label: nil},
	})
	q := quad.Quad{Subject: quad.IRI("name"), Predicate: quad.IRI(rdfs.SubPropertyOf), Object: quad.IRI("personal"), Label: nil}
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	if store.GetProperty(quad.IRI("name")).IsSubPropertyOf(store.GetProperty(quad.IRI("personal"))) {
		t.Error("Property was not unregistered as subproperty of super property")
	}
}

func TestDeleteIsSubPropertyOfRecursive(t *testing.T) {
	store := NewStore()
	store.ProcessQuads([]quad.Quad{
		quad.Quad{Subject: quad.IRI("name"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdf.Property), Label: nil},
		quad.Quad{Subject: quad.IRI("personal"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdf.Property), Label: nil},
		quad.Quad{Subject: quad.IRI("information"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI(rdf.Property), Label: nil},
	})
	quads := []quad.Quad{
		quad.Quad{Subject: quad.IRI("name"), Predicate: quad.IRI(rdfs.SubPropertyOf), Object: quad.IRI("personal"), Label: nil},
		quad.Quad{Subject: quad.IRI("personal"), Predicate: quad.IRI(rdfs.SubPropertyOf), Object: quad.IRI("information"), Label: nil},
	}
	store.ProcessQuads(quads)
	store.UnprocessQuads(quads)
	if store.GetProperty(quad.IRI("name")).IsSubPropertyOf(store.GetProperty(quad.IRI("information"))) {
		t.Error("Property was not unregistered as subproperty of super property")
	}
}

func TestClassRemoveReference(t *testing.T) {
	store := NewStore()
	q := quad.Quad{Subject: quad.IRI("alice"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI("Person"), Label: nil}
	store.ProcessQuad(q)
	class := store.GetClass(quad.IRI("Person"))
	class.removeReference()
	if store.GetClass(quad.IRI("Person")) != nil {
		t.Error("class was not garbage collected")
	}
}
func TestPropertyRemoveReference(t *testing.T) {
	store := NewStore()
	q := quad.Quad{Subject: quad.IRI("alice"), Predicate: quad.IRI("likes"), Object: quad.IRI("bob"), Label: nil}
	store.ProcessQuad(q)
	class := store.GetProperty(quad.IRI("likes"))
	class.removeReference()
	if store.GetClass(quad.IRI("likes")) != nil {
		t.Error("property was not garbage collected")
	}
}

func TestClassDereference(t *testing.T) {
	store := NewStore()
	q := quad.Quad{Subject: quad.IRI("alice"), Predicate: quad.IRI(rdf.Type), Object: quad.IRI("Person"), Label: nil}
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	if store.GetClass(quad.IRI("Person")) != nil {
		t.Error("class was not garbage collected")
	}
}

func TestPropertyDereference(t *testing.T) {
	store := NewStore()
	q := quad.Quad{Subject: quad.IRI("alice"), Predicate: quad.IRI("likes"), Object: quad.IRI("bob"), Label: nil}
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	if store.GetProperty(quad.IRI("likes")) != nil {
		t.Error("property was not garbage collected")
	}
}
