package inference

import (
	"testing"

	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc/rdf"
	"github.com/cayleygraph/quad/voc/rdfs"
	"github.com/stretchr/testify/require"
)

func Triple(subject quad.Value, predicate quad.IRI, object quad.Value) quad.Quad {
	return quad.Quad{Subject: subject, Predicate: predicate, Object: object}
}

var (
	domain           = quad.IRI(rdfs.Domain)
	_range           = quad.IRI(rdfs.Range)
	_type            = quad.IRI(rdf.Type)
	class            = quad.IRI(rdfs.Class)
	literal          = quad.IRI(rdfs.Literal)
	property         = quad.IRI(rdf.Property)
	subClassOf       = quad.IRI(rdfs.SubClassOf)
	subPropertyOf    = quad.IRI(rdfs.SubPropertyOf)
	alice            = quad.IRI("alice")
	bob              = quad.IRI("bob")
	engineer         = quad.IRI("Engineer")
	information      = quad.IRI("information")
	likes            = quad.IRI("likes")
	name             = quad.IRI("name")
	person           = quad.IRI("Person")
	personal         = quad.IRI("personal")
	softwareEngineer = quad.IRI("SoftwareEngineer")
)
var (
	aliceIsPerson                    = Triple(alice, _type, person)
	aliceLikesBob                    = Triple(alice, likes, bob)
	engineerClass                    = Triple(engineer, _type, class)
	engineerSubClass                 = Triple(engineer, subClassOf, person)
	nameDomainPerson                 = Triple(name, domain, person)
	nameProperty                     = Triple(name, _type, property)
	nameSubPropertyOfPersonal        = Triple(name, subPropertyOf, personal)
	personalProperty                 = Triple(personal, _type, property)
	personalSubPropertyOfInformation = Triple(personal, subPropertyOf, information)
	personClass                      = Triple(person, _type, class)
	softwareEngineerClass            = Triple(softwareEngineer, _type, class)
)
var (
	engineerAndSoftwareEngineerSubClasses = []quad.Quad{
		engineerSubClass,
		Triple(softwareEngineer, subClassOf, engineer),
	}
	engineerAndPersonClasses = []quad.Quad{
		engineerClass,
		personClass,
	}
)

func TestClassName(t *testing.T) {
	iri := alice
	c := Class{name: iri}
	require.Equal(t, c.Name(), iri, "Name was not set correctly for the class")
}

func TestPropertyName(t *testing.T) {
	iri := likes
	p := Property{name: iri}
	require.Equal(t, p.Name(), iri, "Name was not set correctly for the property")
}

func TestReferencedType(t *testing.T) {
	store := NewStore()
	q := aliceIsPerson
	store.ProcessQuad(q)
	createdClass := store.GetClass(person)
	require.NotNil(t, createdClass, "Class was not created")
}

func TestReferencedBNodeType(t *testing.T) {
	store := NewStore()
	name := quad.BNode("123")
	q := Triple(alice, _type, name)
	store.ProcessQuad(q)
	createdClass := store.GetClass(name)
	require.NotNil(t, createdClass, "Class was not created")
}

func TestReferencedProperty(t *testing.T) {
	store := NewStore()
	q := aliceLikesBob
	store.ProcessQuad(q)
	createdProperty := store.GetProperty(likes)
	require.NotNil(t, createdProperty, "Property was not created")
}

func TestNewClass(t *testing.T) {
	store := NewStore()
	q := personClass
	store.ProcessQuad(q)
	createdClass := store.GetClass(person)
	require.NotNil(t, createdClass, "Class was not created")
}

func TestNewBNodeClass(t *testing.T) {
	store := NewStore()
	name := quad.BNode("123")
	q := Triple(name, _type, class)
	store.ProcessQuad(q)
	createdClass := store.GetClass(name)
	require.NotNil(t, createdClass, "Class was not created")
}

func TestInvalidNewClass(t *testing.T) {
	store := NewStore()
	name := quad.String("Foo")
	q := Triple(alice, _type, name)
	store.ProcessQuad(q)
	createdClass := store.GetClass(name)
	require.Nil(t, createdClass, "Invalid class was created")
}

func TestNewProperty(t *testing.T) {
	store := NewStore()
	q := nameProperty
	store.ProcessQuad(q)
	createdProperty := store.GetProperty(name)
	require.NotNil(t, createdProperty, "Property was not created")
}

func TestInvalidNewProperty(t *testing.T) {
	store := NewStore()
	name := quad.String("Foo")
	q := quad.Quad{Subject: alice, Predicate: name, Object: bob}
	store.ProcessQuad(q)
	createdProperty := store.GetProperty(name)
	require.Nil(t, createdProperty, "Invalid property was created")
}

func TestSubClass(t *testing.T) {
	store := NewStore()
	q := engineerSubClass
	store.ProcessQuad(q)
	createdClass := store.GetClass(engineer)
	createdSuperClass := store.GetClass(person)
	require.NotNil(t, createdClass, "Class was not created")
	require.NotNil(t, createdSuperClass, "Super class was not created")
	if _, ok := createdClass.super[createdSuperClass]; !ok {
		t.Error("Super class was not registered for class")
	}
	if _, ok := createdSuperClass.sub[createdClass]; !ok {
		t.Error("Class was not registered for super class")
	}
}

func TestSubProperty(t *testing.T) {
	store := NewStore()
	q := nameSubPropertyOfPersonal
	store.ProcessQuad(q)
	createdProperty := store.GetProperty(name)
	createdSuperProperty := store.GetProperty(personal)
	require.NotNil(t, createdProperty, "Property was not created")
	require.NotNil(t, createdSuperProperty, "Super property was not created")
	if _, ok := createdProperty.super[createdSuperProperty]; !ok {
		t.Error("Super property was not registered for property")
	}
	if _, ok := createdSuperProperty.sub[createdProperty]; !ok {
		t.Error("Property was not registered for super property")
	}
}

func TestPropertyDomain(t *testing.T) {
	store := NewStore()
	q := nameDomainPerson
	store.ProcessQuad(q)
	createdProperty := store.GetProperty(name)
	createdClass := store.GetClass(person)
	require.NotNil(t, createdProperty, "Property was not created")
	require.NotNil(t, createdClass, "Domain class was not created")
	if createdProperty.Domain() != createdClass {
		t.Error("Domain class was not registered for property")
	}
	if _, ok := createdClass.ownProperties[createdProperty]; !ok {
		t.Error("Property was not registered for class")
	}
}

func TestPropertyRange(t *testing.T) {
	store := NewStore()
	q := Triple(name, _range, person)
	store.ProcessQuad(q)
	createdProperty := store.GetProperty(name)
	createdClass := store.GetClass(person)
	require.NotNil(t, createdProperty, "Property was not created")
	require.NotNil(t, createdClass, "Range class was not created")
	if createdProperty.Range() != createdClass {
		t.Error("Range class was not registered for property")
	}
	if _, ok := createdClass.inProperties[createdProperty]; !ok {
		t.Error("Property was not registered for class")
	}
}

func TestIsSubClassOf(t *testing.T) {
	store := NewStore()
	q := engineerSubClass
	store.ProcessQuad(q)
	if !store.GetClass(engineer).IsSubClassOf(store.GetClass(person)) {
		t.Error("Class was not registered as subclass of super class")
	}
}

func TestIsSubClassOfRecursive(t *testing.T) {
	store := NewStore()
	quads := engineerAndSoftwareEngineerSubClasses
	store.ProcessQuads(quads)
	if !store.GetClass(softwareEngineer).IsSubClassOf(store.GetClass(person)) {
		t.Error("Class was not registered as subclass of super class")
	}
}

func TestIsSubClassOfItself(t *testing.T) {
	store := NewStore()
	q := personClass
	store.ProcessQuad(q)
	if !store.GetClass(person).IsSubClassOf(store.GetClass(person)) {
		t.Error("IsSubClassOf itself doesn't work")
	}
}

func TestIsSubClassOfResource(t *testing.T) {
	store := NewStore()
	q := personClass
	store.ProcessQuad(q)
	if !store.GetClass(person).IsSubClassOf(store.GetClass(quad.IRI(rdfs.Resource))) {
		t.Error("ItSubClassOf rdfs:Resource doesn't work")
	}
}

func TestIsSubPropertyOf(t *testing.T) {
	store := NewStore()
	q := nameSubPropertyOfPersonal
	store.ProcessQuad(q)
	if !store.GetProperty(name).IsSubPropertyOf(store.GetProperty(personal)) {
		t.Error("Property was not registered as subproperty of super property")
	}
}

func TestIsSubPropertyOfRecursive(t *testing.T) {
	store := NewStore()
	quads := []quad.Quad{
		nameSubPropertyOfPersonal,
		personalSubPropertyOfInformation,
	}
	store.ProcessQuads(quads)
	if !store.GetProperty(name).IsSubPropertyOf(store.GetProperty(information)) {
		t.Error("Property was not registered as subproperty of super property")
	}
}

func TestIsSubPropertyOfItself(t *testing.T) {
	store := NewStore()
	q := nameProperty
	store.ProcessQuad(q)
	if !store.GetProperty(name).IsSubPropertyOf(store.GetProperty(name)) {
		t.Error("IsSubPropertyOf itself doesn't work")
	}
}

func TestUnprocessInvalidQuad(t *testing.T) {
	store := NewStore()
	store.UnprocessQuad(quad.Quad{Subject: alice, Predicate: quad.String("Foo"), Object: person, Label: nil})
}

func TestUnprocessInvalidTypeQuad(t *testing.T) {
	store := NewStore()
	store.UnprocessQuad(quad.Quad{Subject: alice, Predicate: _type, Object: quad.String("Foo"), Label: nil})
}

func TestDeleteReferencedType(t *testing.T) {
	store := NewStore()
	q := aliceIsPerson
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	createdClass := store.GetClass(person)
	require.Nil(t, createdClass, "Class was not deleted")
}

func TestDeleteClassWithSubClass(t *testing.T) {
	store := NewStore()
	store.ProcessQuads([]quad.Quad{
		engineerClass,
		engineerSubClass,
	})
	q := personClass
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	subClass := store.GetClass(engineer)
	if len(subClass.super) != 0 {
		t.Error("Class was not unreferenced")
	}
}

func TestDeleteClassWithSuperClass(t *testing.T) {
	store := NewStore()
	store.ProcessQuads([]quad.Quad{
		personClass,
		engineerSubClass,
	})
	q := engineerClass
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	superClass := store.GetClass(person)
	if len(superClass.sub) != 0 {
		t.Error("Class was not unreferenced")
	}
}

func TestDeleteNewClass(t *testing.T) {
	store := NewStore()
	q := personClass
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	createdClass := store.GetClass(person)
	require.Nil(t, createdClass, "Class was not deleted")
}

func TestDeleteNewProperty(t *testing.T) {
	store := NewStore()
	q := nameProperty
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	createdProperty := store.GetProperty(name)
	require.Nil(t, createdProperty, "Property was not deleted")
}

func TestDeletePropertyWithSubProperty(t *testing.T) {
	store := NewStore()
	store.ProcessQuads([]quad.Quad{
		nameProperty,
		nameSubPropertyOfPersonal,
	})
	q := personalProperty
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	subProperty := store.GetProperty(name)
	if len(subProperty.super) != 0 {
		t.Error("Property was not unreferenced")
	}
}

func TestDeletePropertyWithSuperProperty(t *testing.T) {
	store := NewStore()
	store.ProcessQuads([]quad.Quad{
		personalProperty,
		nameSubPropertyOfPersonal,
	})
	q := nameProperty
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	superProperty := store.GetProperty(personal)
	if len(superProperty.sub) != 0 {
		t.Error("Property was not unreferenced")
	}
}

func TestDeleteSubClass(t *testing.T) {
	store := NewStore()
	store.ProcessQuads(engineerAndPersonClasses)
	q := engineerSubClass
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	createdClass := store.GetClass(engineer)
	createdSuperClass := store.GetClass(person)
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
		nameProperty,
		personalProperty,
	})
	q := nameSubPropertyOfPersonal
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	createdProperty := store.GetProperty(name)
	createdSuperProperty := store.GetProperty(personal)
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
		nameProperty,
		personClass,
	})
	q := nameDomainPerson
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	createdProperty := store.GetProperty(name)
	createdClass := store.GetClass(person)
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
		nameProperty,
		quad.Quad{Subject: literal, Predicate: _type, Object: class, Label: nil},
	})
	q := quad.Quad{Subject: name, Predicate: _range, Object: literal, Label: nil}
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	createdProperty := store.GetProperty(name)
	createdClass := store.GetClass(literal)
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
	store.ProcessQuads(engineerAndPersonClasses)
	q := engineerSubClass
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	if store.GetClass(engineer).IsSubClassOf(store.GetClass(person)) {
		t.Error("Class was not unregistered as subclass of super class")
	}
}

func TestDeleteIsSubClassOfRecursive(t *testing.T) {
	store := NewStore()
	store.ProcessQuads([]quad.Quad{
		engineerClass,
		personClass,
		softwareEngineerClass,
	})
	quads := engineerAndSoftwareEngineerSubClasses
	store.ProcessQuads(quads)
	store.UnprocessQuads(quads)
	if store.GetClass(softwareEngineer).IsSubClassOf(store.GetClass(person)) {
		t.Error("Class was not unregistered as subclass of super class")
	}
}

func TestDeleteIsSubPropertyOf(t *testing.T) {
	store := NewStore()
	store.ProcessQuads([]quad.Quad{
		nameProperty,
		personalProperty,
	})
	q := nameSubPropertyOfPersonal
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	if store.GetProperty(name).IsSubPropertyOf(store.GetProperty(personal)) {
		t.Error("Property was not unregistered as subproperty of super property")
	}
}

func TestDeleteIsSubPropertyOfRecursive(t *testing.T) {
	store := NewStore()
	store.ProcessQuads([]quad.Quad{
		nameProperty,
		personalProperty,
		quad.Quad{Subject: information, Predicate: _type, Object: property, Label: nil},
	})
	quads := []quad.Quad{
		nameSubPropertyOfPersonal,
		personalSubPropertyOfInformation,
	}
	store.ProcessQuads(quads)
	store.UnprocessQuads(quads)
	if store.GetProperty(name).IsSubPropertyOf(store.GetProperty(information)) {
		t.Error("Property was not unregistered as subproperty of super property")
	}
}

func TestClassIsReference(t *testing.T) {
	store := NewStore()
	q := aliceIsPerson
	store.ProcessQuad(q)
	class := store.GetClass(person)
	if !class.isReferenced() {
		t.Error("Class should be referenced")
	}
}

func TestPropertyIsReference(t *testing.T) {
	store := NewStore()
	q := aliceLikesBob
	store.ProcessQuad(q)
	property := store.GetProperty(likes)
	if !property.isReferenced() {
		t.Error("Property should be referenced")
	}
}

func TestClassUnreference(t *testing.T) {
	store := NewStore()
	q := aliceIsPerson
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	require.Nil(t, store.GetClass(person), "class was not garbage collected")
}

func TestPropertyUnreference(t *testing.T) {
	store := NewStore()
	q := aliceLikesBob
	store.ProcessQuad(q)
	store.UnprocessQuad(q)
	require.Nil(t, store.GetProperty(likes), "property was not garbage collected")
}
