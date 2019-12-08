// Package inference implements an in-memory store for inference
//
// RDFS Rules:
//
//		1. (x p y) -> (p rdf:type rdf:Property)
//		2. (p rdfs:domain c), (x p y) -> (x rdf:type c)
//		3. (p rdfs:range c), (x p y) -> (y rdf:type c)
//		4a. (x p y) -> (x rdf:type rdfs:Resource)
//		4b. (x p y) -> (y rdf:type rdfs:Resource)
//		5. (p rdfs:subPropertyOf q), (q rdfs:subPropertyOf r) -> (p rdfs:subPropertyOf r)
//		6. (p rdf:type Property) -> (p rdfs:subPropertyOf p)
//		7. (p rdf:subPropertyOf q), (x p y) -> (x q y)
//		8. (c rdf:type rdfs:Class) -> (c rdfs:subClassOf rdfs:Resource)
//		9. (c rdfs:subClassOf d), (x rdf:type c) -> (x rdf:type d)
//		10. (c rdf:type rdfs:Class) -> (c rdfs:subClassOf c)
//		11. (c rdfs:subClassOf d), (d rdfs:subClassOf e) -> (c rdfs:subClassOf e)
//		12. (p rdf:type rdfs:ContainerMembershipProperty) -> (p rdfs:subPropertyOf rdfs:member)
//		13. (x rdf:type rdfs:Datatype) -> (x rdfs:subClassOf rdfs:Literal)
//		Exported from: https://www.researchgate.net/figure/RDF-RDFS-entailment-rules_tbl1_268419911
// Implemented here:
// 1 5 6 8 10 11
package inference

import (
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc/rdf"
	"github.com/cayleygraph/quad/voc/rdfs"
)

// ClassSet is a set of RDF Classes
type ClassSet map[*Class]struct{}

// PropertySet is a set of RDF Properties
type PropertySet map[*Property]struct{}

// Class represents a RDF Class with the links to classes and other properties
type Class struct {
	name          quad.Value
	explicit      bool
	references    int
	super         ClassSet
	sub           ClassSet
	ownProperties PropertySet
	inProperties  PropertySet
	store         *Store
}

func newClass(name quad.Value, explicit bool, store *Store) *Class {
	return &Class{
		name:          name,
		explicit:      explicit,
		super:         make(ClassSet),
		sub:           make(ClassSet),
		ownProperties: make(PropertySet),
		inProperties:  make(PropertySet),
		store:         store,
	}
}

// Name returns the class's name
func (class *Class) Name() quad.Value {
	return class.name
}

// IsSubClassOf recursively checks whether class is a superClass
func (class *Class) IsSubClassOf(superClass *Class) bool {
	if class == superClass {
		return true
	}
	if superClass.name == quad.IRI(rdfs.Resource) {
		return true
	}
	if _, ok := class.super[superClass]; ok {
		return true
	}
	for s := range class.super {
		if s.IsSubClassOf(superClass) {
			return true
		}
	}
	return false
}

func (class *Class) isReferenced() bool {
	return class.explicit || len(class.super) > 0 ||
		len(class.sub) > 0 ||
		len(class.ownProperties) > 0 ||
		len(class.inProperties) > 0 ||
		class.references > 0
}

func (class *Class) deleteIfUnreferenced() {
	if class != nil && !class.isReferenced() {
		class.store.deleteClass(class.name)
	}
}

// Property represents a RDF Property with the links to classes and other properties
type Property struct {
	name       quad.Value
	explicit   bool
	references int
	domain     *Class
	_range     *Class
	super      PropertySet
	sub        PropertySet
	store      *Store
}

func newProperty(name quad.Value, explicit bool, store *Store) *Property {
	return &Property{
		name:     name,
		explicit: explicit,
		super:    make(PropertySet),
		sub:      make(PropertySet),
		store:    store,
	}
}

// Name returns the property's name
func (property *Property) Name() quad.Value {
	return property.name
}

// Domain returns the domain of the property
func (property *Property) Domain() *Class {
	return property.domain
}

// Range returns the range of the property
func (property *Property) Range() *Class {
	return property._range
}

// IsSubPropertyOf recursively checks whether property is a superProperty
func (property *Property) IsSubPropertyOf(superProperty *Property) bool {
	if property == superProperty {
		return true
	}
	if _, ok := property.super[superProperty]; ok {
		return true
	}
	for s := range property.super {
		if s.IsSubPropertyOf(superProperty) {
			return true
		}
	}
	return false
}

func (property *Property) isReferenced() bool {
	return property.explicit || property.references > 0 ||
		len(property.super) > 0 ||
		len(property.sub) > 0
}

func (property *Property) deleteIfUnreferenced() {
	if property != nil && !property.isReferenced() {
		property.store.deleteProperty(property.name)
	}
}

// Store is a struct holding the inference data
type Store struct {
	classes    map[quad.Value]*Class
	properties map[quad.Value]*Property
}

// NewStore creates a new Store
func NewStore() Store {
	store := Store{
		classes:    map[quad.Value]*Class{},
		properties: map[quad.Value]*Property{},
	}
	rdfsResource := newClass(quad.IRI(rdfs.Resource), true, &store)
	store.classes[rdfsResource.name] = rdfsResource
	return store
}

// GetClass returns a class struct for class name, if it doesn't exist in the store then it returns nil
func (store *Store) GetClass(name quad.Value) *Class {
	return store.classes[name]
}

// GetProperty returns a class struct for property name, if it doesn't exist in the store then it returns nil
func (store *Store) GetProperty(name quad.Value) *Property {
	return store.properties[name]
}

func (store *Store) createClass(name quad.Value) {
	class, ok := store.classes[name]
	if !ok {
		store.classes[name] = newClass(name, true, store)
	} else {
		class.explicit = true
	}
}

func (store *Store) getOrCreateImplicitClass(name quad.Value) *Class {
	class, ok := store.classes[name]
	if !ok {
		class = newClass(name, false, store)
		store.classes[name] = class
	}
	return class
}

func (store *Store) createProperty(name quad.Value) {
	if property, ok := store.properties[name]; ok {
		property.explicit = true
		return
	}
	store.properties[name] = newProperty(name, true, store)
}

func (store *Store) getOrCreateImplicitProperty(name quad.Value) *Property {
	if property, ok := store.properties[name]; ok {
		return property
	}
	property := newProperty(name, false, store)
	store.properties[name] = property
	return property
}

func (store *Store) addClassRelationship(child quad.Value, parent quad.Value) {
	parentClass := store.getOrCreateImplicitClass(parent)
	childClass := store.getOrCreateImplicitClass(child)
	if _, ok := parentClass.sub[childClass]; !ok {
		parentClass.sub[childClass] = struct{}{}
		childClass.super[parentClass] = struct{}{}
	}
}

func (store *Store) addPropertyRelationship(child quad.Value, parent quad.Value) {
	parentProperty := store.getOrCreateImplicitProperty(parent)
	childProperty := store.getOrCreateImplicitProperty(child)
	if _, ok := parentProperty.sub[childProperty]; !ok {
		parentProperty.sub[childProperty] = struct{}{}
		childProperty.super[parentProperty] = struct{}{}
	}
}

func (store *Store) setPropertyDomain(property quad.Value, domain quad.Value) {
	p := store.getOrCreateImplicitProperty(property)
	class := store.getOrCreateImplicitClass(domain)
	// FIXME(iddan): Currently doesn't support multiple domains as they are very rare
	p.domain = class
	class.ownProperties[p] = struct{}{}
}

func (store *Store) setPropertyRange(property quad.Value, _range quad.Value) {
	p := store.getOrCreateImplicitProperty(property)
	class := store.getOrCreateImplicitClass(_range)
	p._range = class
	// FIXME(iddan): Currently doesn't support multiple ranges as they are very rare
	class.inProperties[p] = struct{}{}
}

func (store *Store) addClassInstance(name quad.Value) {
	class := store.GetClass(name)
	if class == nil {
		class = store.getOrCreateImplicitClass(name)
	}
	class.references++
}

func (store *Store) addPropertyInstance(name quad.Value) {
	property := store.GetProperty(name)
	if property == nil {
		property = store.getOrCreateImplicitProperty(name)
	}
	property.references++
}

// ProcessQuad is used to update the store with a new quad
func (store *Store) ProcessQuad(q quad.Quad) {
	subject, predicate, object := q.Subject, q.Predicate, q.Object
	predicateIRI, ok := predicate.(quad.IRI)
	if !ok {
		return
	}
	switch predicateIRI {
	case rdf.Type:
		if _, ok := object.(quad.BNode); ok {
			store.addClassInstance(object)
		}
		objectIRI, ok := object.(quad.IRI)
		if !ok {
			return
		}
		switch objectIRI {
		case rdfs.Class:
			store.createClass(subject)
		case rdf.Property:
			store.createProperty(subject)
		default:
			store.addClassInstance(object)
		}
	case rdfs.SubPropertyOf:
		store.addPropertyRelationship(subject, object)
	case rdfs.SubClassOf:
		store.addClassRelationship(subject, object)
	case rdfs.Domain:
		store.setPropertyDomain(subject, object)
	case rdfs.Range:
		store.setPropertyRange(subject, object)
	default:
		store.addPropertyInstance(predicate)
	}
}

// ProcessQuads is used to update the store with multiple quads
func (store *Store) ProcessQuads(quads []quad.Quad) {
	for _, q := range quads {
		store.ProcessQuad(q)
	}
}

func (store *Store) deleteClass(name quad.Value) {
	if class, ok := store.classes[name]; ok {
		for sub := range class.sub {
			delete(sub.super, class)
		}
		for super := range class.super {
			delete(super.sub, class)
		}
		delete(store.classes, name)
	}
}

func (store *Store) deleteProperty(name quad.Value) {
	if property, ok := store.properties[name]; ok {
		for super := range property.super {
			delete(super.sub, property)
		}
		for sub := range property.sub {
			delete(sub.super, property)
		}
		delete(store.properties, name)
	}
}

func (store *Store) deleteClassRelationship(child quad.Value, parent quad.Value) {
	parentClass := store.GetClass(parent)
	childClass := store.GetClass(child)
	if _, ok := parentClass.sub[childClass]; ok {
		delete(parentClass.sub, childClass)
		delete(childClass.super, parentClass)
		parentClass.deleteIfUnreferenced()
		childClass.deleteIfUnreferenced()
	}
}

func (store *Store) deletePropertyRelationship(child quad.Value, parent quad.Value) {
	parentProperty := store.GetProperty(parent)
	childProperty := store.GetProperty(child)
	if _, ok := parentProperty.sub[childProperty]; ok {
		delete(parentProperty.sub, childProperty)
		delete(childProperty.super, parentProperty)
		parentProperty.deleteIfUnreferenced()
		childProperty.deleteIfUnreferenced()
	}
}

func (store *Store) unsetPropertyDomain(property quad.Value, domain quad.Value) {
	p := store.GetProperty(property)
	class := store.GetClass(domain)
	// FIXME(iddan): Currently doesn't support multiple domains as they are very rare
	p.domain = nil
	delete(class.ownProperties, p)
	p.deleteIfUnreferenced()
	class.deleteIfUnreferenced()
}

func (store *Store) unsetPropertyRange(property quad.Value, _range quad.Value) {
	p := store.GetProperty(property)
	class := store.GetClass(_range)
	p._range = nil
	// FIXME(iddan): Currently doesn't support multiple ranges as they are very rare
	delete(class.inProperties, p)
	p.deleteIfUnreferenced()
	class.deleteIfUnreferenced()
}

func (store *Store) deleteClassInstance(name quad.Value) {
	class := store.GetClass(name)
	if class != nil {
		class.references--
	}
	class.deleteIfUnreferenced()
}

func (store *Store) deletePropertyInstance(name quad.Value) {
	property := store.GetProperty(name)
	if property != nil {
		property.references--
	}
	property.deleteIfUnreferenced()
}

// UnprocessQuad is used to delete a quad from the store
func (store *Store) UnprocessQuad(q quad.Quad) {
	subject, predicate, object := q.Subject, q.Predicate, q.Object
	predicateIRI, ok := predicate.(quad.IRI)
	if !ok {
		return
	}
	switch predicateIRI {
	case rdf.Type:
		objectIRI, ok := object.(quad.IRI)
		if !ok {
			return
		}
		switch objectIRI {
		case rdfs.Class:
			store.deleteClass(subject)
		case rdf.Property:
			store.deleteProperty(subject)
		default:
			store.deleteClassInstance(object)
		}
	case rdfs.SubPropertyOf:
		store.deletePropertyRelationship(subject, object)
	case rdfs.SubClassOf:
		store.deleteClassRelationship(subject, object)
	case rdfs.Domain:
		store.unsetPropertyDomain(subject, object)
	case rdfs.Range:
		store.unsetPropertyRange(subject, object)
	default:
		store.deletePropertyInstance(predicate)
	}
}

// UnprocessQuads is used to delete multiple quads from the store
func (store *Store) UnprocessQuads(quads []quad.Quad) {
	for _, q := range quads {
		store.UnprocessQuad(q)
	}
}
