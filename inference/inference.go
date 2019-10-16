package inference

import (
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc/rdf"
	"github.com/cayleygraph/quad/voc/rdfs"
)

// Class represents a RDF Class with the links to classes and other properties
type Class struct {
	name          quad.Value
	references    int
	super         map[*Class]struct{}
	sub           map[*Class]struct{}
	ownProperties map[*Property]struct{}
	inProperties  map[*Property]struct{}
	store         *Store
}

// Name returns the class's name
func (class *Class) Name() quad.Value {
	return class.name
}

// IsSubClassOf recursively checks whether class is a superClass
func (class *Class) IsSubClassOf(superClass *Class) bool {
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

func (class *Class) removeReference() {
	class.references--
	if class.references == 0 {
		delete(class.store.classes, class.name)
	}
}

// Property represents a RDF Property with the links to classes and other properties
type Property struct {
	name       quad.Value
	references int
	domain     *Class
	_range     *Class
	super      map[*Property]struct{}
	sub        map[*Property]struct{}
	store      *Store
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

func (property *Property) removeReference() {
	property.references--
	if property.references == 0 {
		delete(property.store.properties, property.name)
	}
}

// Store is a struct holding the inference data
type Store struct {
	classes    map[quad.Value]*Class
	properties map[quad.Value]*Property
}

// NewStore creates a new Store
func NewStore() Store {
	return Store{
		classes:    map[quad.Value]*Class{},
		properties: map[quad.Value]*Property{},
	}
}

// GetClass returns a class struct for class name, if it doesn't exist in the store then it returns nil
func (store *Store) GetClass(name quad.Value) *Class {
	return store.classes[name]
}

// GetProperty returns a class struct for property name, if it doesn't exist in the store then it returns nil
func (store *Store) GetProperty(name quad.Value) *Property {
	return store.properties[name]
}

func (store *Store) addClass(class quad.Value) *Class {
	if c, ok := store.classes[class]; ok {
		c.references++
		return c
	}
	c := &Class{
		name:          class,
		references:    1,
		super:         map[*Class]struct{}{},
		sub:           map[*Class]struct{}{},
		ownProperties: map[*Property]struct{}{},
		inProperties:  map[*Property]struct{}{},
		store:         store,
	}
	store.classes[class] = c
	return c
}

func (store *Store) addProperty(property quad.Value) *Property {
	if p, ok := store.properties[property]; ok {
		p.references++
		return p
	}
	p := &Property{
		name:       property,
		references: 1,
		super:      map[*Property]struct{}{},
		sub:        map[*Property]struct{}{},
		store:      store,
	}
	store.properties[property] = p
	return p
}

func (store *Store) addClassRelationship(child quad.Value, parent quad.Value) {
	parentClass := store.addClass(parent)
	childClass := store.addClass(child)
	if _, ok := parentClass.sub[childClass]; !ok {
		parentClass.sub[childClass] = struct{}{}
		childClass.super[parentClass] = struct{}{}
	}
}

func (store *Store) addPropertyRelationship(child quad.Value, parent quad.Value) {
	parentProperty := store.addProperty(parent)
	childProperty := store.addProperty(child)
	if _, ok := parentProperty.sub[childProperty]; !ok {
		parentProperty.sub[childProperty] = struct{}{}
		childProperty.super[parentProperty] = struct{}{}
	}
}

func (store *Store) setPropertyDomain(property quad.Value, domain quad.Value) {
	p := store.addProperty(property)
	class := store.addClass(domain)
	// FIXME(iddan): Currently doesn't support multiple domains as they are very rare
	p.domain = class
	class.ownProperties[p] = struct{}{}
}

func (store *Store) setPropertyRange(property quad.Value, _range quad.Value) {
	p := store.addProperty(property)
	class := store.addClass(_range)
	p._range = class
	// FIXME(iddan): Currently doesn't support multiple ranges as they are very rare
	class.inProperties[p] = struct{}{}
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
		objectIRI, ok := object.(quad.IRI)
		if !ok {
			return
		}
		switch objectIRI {
		case rdfs.Class:
			store.addClass(subject)
		case rdf.Property:
			store.addProperty(subject)
		default:
			store.addClass(object)
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
		store.addProperty(predicate)
	}
}

// ProcessQuads is used to update the store with multiple quads
func (store *Store) ProcessQuads(quads []quad.Quad) {
	for _, q := range quads {
		store.ProcessQuad(q)
	}
}

func (store *Store) deleteClass(class quad.Value) {
	if c, ok := store.classes[class]; ok {
		for sub := range c.sub {
			c.removeReference()
			delete(sub.super, c)
		}
		for super := range c.super {
			c.removeReference()
			delete(super.sub, c)
		}
		delete(store.classes, class)
	}
}

func (store *Store) deleteProperty(property quad.Value) {
	if p, ok := store.properties[property]; ok {
		for super := range p.super {
			p.removeReference()
			delete(super.sub, p)
		}
		for sub := range p.sub {
			p.removeReference()
			delete(sub.super, p)
		}
		delete(store.properties, property)
	}
}

func (store *Store) deleteClassRelationship(child quad.Value, parent quad.Value) {
	parentClass := store.GetClass(parent)
	childClass := store.GetClass(child)
	if _, ok := parentClass.sub[childClass]; ok {
		parentClass.removeReference()
		delete(parentClass.sub, childClass)
		childClass.removeReference()
		delete(childClass.super, parentClass)
	}
}

func (store *Store) deletePropertyRelationship(child quad.Value, parent quad.Value) {
	parentProperty := store.GetProperty(parent)
	childProperty := store.GetProperty(child)
	if _, ok := parentProperty.sub[childProperty]; ok {
		parentProperty.removeReference()
		delete(parentProperty.sub, childProperty)
		childProperty.removeReference()
		delete(childProperty.super, parentProperty)
	}
}

func (store *Store) unsetPropertyDomain(property quad.Value, domain quad.Value) {
	p := store.GetProperty(property)
	class := store.GetClass(domain)
	// FIXME(iddan): Currently doesn't support multiple domains as they are very rare
	p.domain = nil
	delete(class.ownProperties, p)
	p.removeReference()
	class.removeReference()
}

func (store *Store) unsetPropertyRange(property quad.Value, _range quad.Value) {
	p := store.GetProperty(property)
	class := store.GetClass(_range)
	p._range = nil
	// FIXME(iddan): Currently doesn't support multiple ranges as they are very rare
	delete(class.inProperties, p)
	p.removeReference()
	class.removeReference()
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
			store.deleteClass(object)
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
		store.deleteProperty(predicate)
	}
}

// UnprocessQuads is used to delete multiple quads from the store
func (store *Store) UnprocessQuads(quads []quad.Quad) {
	for _, q := range quads {
		store.UnprocessQuad(q)
	}
}
