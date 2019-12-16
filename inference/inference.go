// Package inference implements an in-memory store for inference.
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
//
// Exported from: https://www.researchgate.net/figure/RDF-RDFS-entailment-rules_tbl1_268419911
//
// Implemented here: 1 2 3 5 6 8 10 11
package inference

import (
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc/rdf"
	"github.com/cayleygraph/quad/voc/rdfs"
)

// classSet is a set of RDF Classes
type classSet map[*Class]struct{}

// propertySet is a set of RDF Properties
type propertySet map[*Property]struct{}

// Class represents a RDF Class with the links to classes and other properties
type Class struct {
	store      *Store
	name       quad.Value
	explicit   bool
	references int
	super      classSet
	sub        classSet
	ownProp    propertySet
	inProp     propertySet
}

func (s *Store) newClass(name quad.Value, explicit bool) *Class {
	c := &Class{
		store:    s,
		name:     name,
		explicit: explicit,
		super:    make(classSet),
		sub:      make(classSet),
		ownProp:  make(propertySet),
		inProp:   make(propertySet),
	}
	s.classes[name] = c
	return c
}

// Name returns the class's name
func (c *Class) Name() quad.Value {
	return c.name
}

// IsSubClassOf recursively checks whether class is a superClass
func (c *Class) IsSubClassOf(super *Class) bool {
	if c == super {
		return true
	}
	if super.name == quad.IRI(rdfs.Resource) {
		return true
	}
	if _, ok := c.super[super]; ok {
		return true
	}
	for s := range c.super {
		if s.IsSubClassOf(super) {
			return true
		}
	}
	return false
}

func (c *Class) isReferenced() bool {
	return c.explicit || len(c.super) > 0 ||
		len(c.sub) > 0 ||
		len(c.ownProp) > 0 ||
		len(c.inProp) > 0 ||
		c.references > 0
}

func (c *Class) deleteIfUnreferenced() {
	if c != nil && !c.isReferenced() {
		c.store.deleteClass(c.name)
	}
}

// Property represents a RDF Property with the links to classes and other properties
type Property struct {
	name       quad.Value
	explicit   bool
	references int
	domain     *Class
	prange     *Class
	super      propertySet
	sub        propertySet
	store      *Store
}

func newProperty(name quad.Value, explicit bool, store *Store) *Property {
	return &Property{
		name:     name,
		explicit: explicit,
		super:    make(propertySet),
		sub:      make(propertySet),
		store:    store,
	}
}

// Name returns the property's name
func (p *Property) Name() quad.Value {
	return p.name
}

// Domain returns the domain of the property
func (p *Property) Domain() *Class {
	return p.domain
}

// Range returns the range of the property
func (p *Property) Range() *Class {
	return p.prange
}

// IsSubPropertyOf recursively checks whether property is a superProperty
func (p *Property) IsSubPropertyOf(super *Property) bool {
	if p == super {
		return true
	}
	if _, ok := p.super[super]; ok {
		return true
	}
	for s := range p.super {
		if s.IsSubPropertyOf(super) {
			return true
		}
	}
	return false
}

func (p *Property) isReferenced() bool {
	return p.explicit || p.references > 0 ||
		len(p.super) > 0 ||
		len(p.sub) > 0 ||
		p.domain != nil ||
		p.prange != nil
}

func (p *Property) deleteIfUnreferenced() {
	if p != nil && !p.isReferenced() {
		p.store.deleteProperty(p.name)
	}
}

// Store is a struct holding the inference data
type Store struct {
	classes    map[quad.Value]*Class
	properties map[quad.Value]*Property
}

// NewStore creates a new Store
func NewStore() Store {
	s := Store{
		classes:    make(map[quad.Value]*Class),
		properties: make(map[quad.Value]*Property),
	}
	s.ensureClass(quad.IRI(rdfs.Resource))
	return s
}

// GetClass returns a class struct for class name, if it doesn't exist in the store then it returns nil
func (s *Store) GetClass(name quad.Value) *Class {
	return s.classes[name]
}

// GetProperty returns a class struct for property name, if it doesn't exist in the store then it returns nil
func (s *Store) GetProperty(name quad.Value) *Property {
	return s.properties[name]
}

func (s *Store) ensureClass(name quad.Value) {
	if c, ok := s.classes[name]; ok {
		c.explicit = true
	} else {
		_ = s.newClass(name, true)
	}
}

func (s *Store) getOrCreateImplicitClass(name quad.Value) *Class {
	c, ok := s.classes[name]
	if !ok {
		c = s.newClass(name, false)
	}
	return c
}

func (s *Store) createProperty(name quad.Value) {
	if property, ok := s.properties[name]; ok {
		property.explicit = true
		return
	}
	s.properties[name] = newProperty(name, true, s)
}

func (s *Store) getOrCreateImplicitProperty(name quad.Value) *Property {
	if p, ok := s.properties[name]; ok {
		return p
	}
	p := newProperty(name, false, s)
	s.properties[name] = p
	return p
}

func (s *Store) addClassRelationship(child quad.Value, parent quad.Value) {
	p := s.getOrCreateImplicitClass(parent)
	c := s.getOrCreateImplicitClass(child)
	if _, ok := p.sub[c]; !ok {
		p.sub[c] = struct{}{}
		c.super[p] = struct{}{}
	}
}

func (s *Store) addPropertyRelationship(child quad.Value, parent quad.Value) {
	p := s.getOrCreateImplicitProperty(parent)
	c := s.getOrCreateImplicitProperty(child)
	if _, ok := p.sub[c]; !ok {
		p.sub[c] = struct{}{}
		c.super[p] = struct{}{}
	}
}

func (s *Store) setPropertyDomain(property quad.Value, domain quad.Value) {
	p := s.getOrCreateImplicitProperty(property)
	c := s.getOrCreateImplicitClass(domain)
	// FIXME(iddan): Currently doesn't support multiple domains as they are very rare
	p.domain = c
	c.ownProp[p] = struct{}{}
}

func (s *Store) setPropertyRange(property quad.Value, prange quad.Value) {
	p := s.getOrCreateImplicitProperty(property)
	c := s.getOrCreateImplicitClass(prange)
	p.prange = c
	// FIXME(iddan): Currently doesn't support multiple ranges as they are very rare
	c.inProp[p] = struct{}{}
}

func (s *Store) addClassInstance(name quad.Value) {
	c := s.GetClass(name)
	if c == nil {
		c = s.getOrCreateImplicitClass(name)
	}
	c.references++
}

func (s *Store) addPropertyInstance(name quad.Value) *Property {
	p := s.GetProperty(name)
	if p == nil {
		p = s.getOrCreateImplicitProperty(name)
	}
	p.references++
	return p
}

// ProcessQuads is used to update the store with multiple quads
func (s *Store) ProcessQuads(quads ...quad.Quad) {
	for _, q := range quads {
		s.processQuad(q)
	}
}

// processQuad is used to update the store with a new quad
func (s *Store) processQuad(q quad.Quad) {
	pred, ok := q.Predicate.(quad.IRI)
	if !ok {
		return
	}
	sub, obj := q.Subject, q.Object
	switch pred {
	case rdf.Type:
		switch obj := obj.(type) {
		case quad.BNode:
			s.addClassInstance(obj)
		case quad.IRI:
			switch obj {
			case rdfs.Class:
				s.ensureClass(sub)
			case rdf.Property:
				s.createProperty(sub)
			default:
				s.addClassInstance(obj)
			}
		}
	case rdfs.SubPropertyOf:
		s.addPropertyRelationship(sub, obj)
	case rdfs.SubClassOf:
		s.addClassRelationship(sub, obj)
	case rdfs.Domain:
		s.setPropertyDomain(sub, obj)
	case rdfs.Range:
		s.setPropertyRange(sub, obj)
	default:
		p := s.addPropertyInstance(pred)
		domain := p.Domain()
		if domain != nil {
			domain.references++
		}
		prange := p.Range()
		if prange != nil {
			prange.references++
		}
	}
}

func (s *Store) deleteClass(name quad.Value) {
	c, ok := s.classes[name]
	if !ok {
		return
	}
	for sub := range c.sub {
		delete(sub.super, c)
	}
	for super := range c.super {
		delete(super.sub, c)
	}
	delete(s.classes, name)
}

func (s *Store) deleteProperty(name quad.Value) {
	p, ok := s.properties[name]
	if !ok {
		return
	}
	for super := range p.super {
		delete(super.sub, p)
	}
	for sub := range p.sub {
		delete(sub.super, p)
	}
	delete(s.properties, name)
}

func (s *Store) deleteClassRel(child quad.Value, parent quad.Value) {
	p := s.GetClass(parent)
	c := s.GetClass(child)
	if _, ok := p.sub[c]; ok {
		delete(p.sub, c)
		delete(c.super, p)
		p.deleteIfUnreferenced()
		c.deleteIfUnreferenced()
	}
}

func (s *Store) deletePropertyRel(child quad.Value, parent quad.Value) {
	p := s.GetProperty(parent)
	c := s.GetProperty(child)
	if _, ok := p.sub[c]; ok {
		delete(p.sub, c)
		delete(c.super, p)
		p.deleteIfUnreferenced()
		c.deleteIfUnreferenced()
	}
}

func (s *Store) unsetPropertyDomain(property quad.Value, domain quad.Value) {
	p := s.GetProperty(property)
	c := s.GetClass(domain)
	// FIXME(iddan): Currently doesn't support multiple domains as they are very rare
	p.domain = nil
	delete(c.ownProp, p)
	p.deleteIfUnreferenced()
	c.deleteIfUnreferenced()
}

func (s *Store) unsetPropertyRange(property quad.Value, prange quad.Value) {
	p := s.GetProperty(property)
	c := s.GetClass(prange)
	p.prange = nil
	// FIXME(iddan): Currently doesn't support multiple ranges as they are very rare
	delete(c.inProp, p)
	p.deleteIfUnreferenced()
	c.deleteIfUnreferenced()
}

func (s *Store) deleteClassInstance(name quad.Value) {
	c := s.GetClass(name)
	if c == nil {
		return
	}
	c.references--
	c.deleteIfUnreferenced()
}

func (s *Store) deletePropertyInstance(name quad.Value) *Property {
	p := s.GetProperty(name)
	if p == nil {
		return nil
	}
	p.references--
	p.deleteIfUnreferenced()
	return p
}

// UnprocessQuads is used to delete multiple quads from the store
func (s *Store) UnprocessQuads(quads ...quad.Quad) {
	for _, q := range quads {
		s.unprocessQuad(q)
	}
}

// unprocessQuad is used to delete a quad from the store
func (s *Store) unprocessQuad(q quad.Quad) {
	pred, ok := q.Predicate.(quad.IRI)
	if !ok {
		return
	}
	sub, obj := q.Subject, q.Object
	switch pred {
	case rdf.Type:
		obj, ok := obj.(quad.IRI)
		if !ok {
			return
		}
		switch obj {
		case rdfs.Class:
			s.deleteClass(sub)
		case rdf.Property:
			s.deleteProperty(sub)
		default:
			s.deleteClassInstance(obj)
		}
	case rdfs.SubPropertyOf:
		s.deletePropertyRel(sub, obj)
	case rdfs.SubClassOf:
		s.deleteClassRel(sub, obj)
	case rdfs.Domain:
		s.unsetPropertyDomain(sub, obj)
	case rdfs.Range:
		s.unsetPropertyRange(sub, obj)
	default:
		p := s.deletePropertyInstance(pred)
		if p != nil {
			if domain := p.Domain(); domain != nil {
				s.deleteClassInstance(domain.Name())
			}
			if prange := p.Range(); prange != nil {
				s.deleteClassInstance(prange.Name())
			}
		}
	}
}
