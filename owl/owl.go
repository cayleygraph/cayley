package owl

import (
	"context"
	"fmt"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc/rdf"
	"github.com/cayleygraph/quad/voc/rdfs"
)

type Class struct {
	ctx        context.Context
	qs         graph.QuadStore
	ref        graph.Ref
	Identifier quad.Value
}

func (c *Class) path() *path.Path {
	return path.StartPath(c.qs, c.Identifier)
}

func (c *Class) unionsPath() *path.Path {
	reverseListPath := path.NewPath(c.qs).In(quad.IRI(rdf.First).Full())
	return c.path().FollowRecursive(reverseListPath, 10, []string{})
}

func classFromRef(ctx context.Context, qs graph.QuadStore, ref graph.Ref) *Class {
	val := qs.NameOf(ref)
	return &Class{
		ctx:        ctx,
		qs:         qs,
		ref:        ref,
		Identifier: val,
	}
}

var domain = quad.IRI(rdfs.Domain).Full()

// Properties return all the properties a class instance may have
func (c *Class) Properties() []*Property {
	ctx := context.TODO()
	p := c.
		path().
		Or(c.unionsPath().In(quad.IRI("http://www.w3.org/2002/07/owl#unionOf").Full())).
		In(domain)
	it := p.BuildIterator(ctx).Iterate()
	var properties []*Property
	for it.Next(ctx) {
		property, err := propertyFromRef(ctx, c.qs, it.Result())
		if err != nil {
			clog.Warningf(err.Error())
			continue
		}
		properties = append(properties, property)
	}
	return properties
}

func (c *Class) ParentClasses() []*Class {
	it := c.path().Out(rdfs.SubClassOf).BuildIterator(c.ctx).Iterate()
	var classes []*Class
	for it.Next(c.ctx) {
		class := classFromRef(c.ctx, c.qs, it.Result())
		classes = append(classes, class)
	}
	return classes
}

func propertyRestrictionsPath(c *Class, property Property, restrictionProperty quad.IRI) *path.Path {
	subClassesPath := c.path().Out(quad.IRI(rdfs.SubClassOf).Full())
	restrictionsPath := subClassesPath.Has(quad.IRI(rdf.Type).Full(), quad.IRI("owl:Restriction").Full())
	propertyRestrictionsPath := restrictionsPath.Has(quad.IRI("owl:onProperty").Full(), property.Identifier)
	return propertyRestrictionsPath.Out(restrictionProperty)
}

func intFromScanner(ctx context.Context, it iterator.Scanner, qs graph.QuadStore) (int, error) {
	for it.Next(ctx) {
		ref := it.Result()
		value := qs.NameOf(ref)
		typedString, ok := value.(quad.TypedString)
		if !ok {
			return -1, fmt.Errorf("Unexpected value %v of type %t", value, value)
		}
		native := typedString.Native()
		i, ok := native.(int)
		if !ok {
			return -1, fmt.Errorf("Unexpected value %v of type %t", native, native)
		}
		return i, nil
	}
	return -1, fmt.Errorf("Iterator has not emitted any value")
}

// CardinalityOf returns the defined exact cardinality for the property for the class
// If exact cardinality is not defined for the class returns an error
func (c *Class) CardinalityOf(property Property) (int, error) {
	p := propertyRestrictionsPath(c, property, quad.IRI("owl:cardinality").Full())
	it := p.BuildIterator(c.ctx).Iterate()
	cardinality, err := intFromScanner(c.ctx, it, c.qs)
	if err != nil {
		return -1, fmt.Errorf("No cardinality is defined for property %v for class %v", property, c)
	}
	return cardinality, nil
}

// MaxCardinalityOf returns the defined max cardinality for the property for the class
// If max cardinality is not defined for the class returns an error
func (c *Class) MaxCardinalityOf(property Property) (int, error) {
	p := propertyRestrictionsPath(c, property, quad.IRI("owl:maxCardinality").Full())
	it := p.BuildIterator(c.ctx).Iterate()
	cardinality, err := intFromScanner(c.ctx, it, c.qs)
	if err != nil {
		return -1, fmt.Errorf("No maxCardinality is defined for property %v for class %v", property, c)
	}
	return cardinality, nil
}

var subClassOf = quad.IRI(rdfs.SubClassOf).Full()

// SubClasses returns all the classes defined as sub classes of the class
func (c *Class) SubClasses() []*Class {
	p := c.path().In(subClassOf)
	it := p.BuildIterator(c.ctx).Iterate()
	var subClasses []*Class
	for it.Next(c.ctx) {
		class := classFromRef(c.ctx, c.qs, it.Result())
		subClasses = append(subClasses, class)
	}
	return subClasses
}

// GetClass returns for given identifier a class object representing a class defined in given store.
// If the identifier is not of a class in the store returns an error.
func GetClass(ctx context.Context, qs graph.QuadStore, identifier quad.IRI) (*Class, error) {
	ref := qs.ValueOf(identifier)
	if ref == nil {
		return nil, fmt.Errorf("Identifier %v does not exist in the store", identifier)
	}
	// TODO(iddan): validate given identifier is an OWL class
	return &Class{Identifier: identifier, ref: ref, qs: qs, ctx: ctx}, nil
}

type Property struct {
	ctx        context.Context
	qs         graph.QuadStore
	ref        graph.Ref
	Identifier quad.IRI
}

func GetProperty(ctx context.Context, qs graph.QuadStore, identifier quad.IRI) (*Property, error) {
	ref := qs.ValueOf(identifier)
	if ref == nil {
		return nil, fmt.Errorf("Identifier %v does not exist in the store", identifier)
	}
	// TODO(iddan): validate given identifier is an OWL property
	return &Property{
		ctx:        ctx,
		qs:         qs,
		ref:        ref,
		Identifier: identifier,
	}, nil
}

func propertyFromRef(ctx context.Context, qs graph.QuadStore, ref graph.Ref) (*Property, error) {
	val := qs.NameOf(ref)
	iri, ok := val.(quad.IRI)
	if !ok {
		return nil, fmt.Errorf("Predicate of unexpected type %t. Predicates should be IRIs", val)
	}
	return &Property{
		ctx:        ctx,
		qs:         qs,
		ref:        ref,
		Identifier: iri,
	}, nil
}

// Range returns the expected target type of a property
func (p *Property) Range() (quad.Value, error) {
	rangePath := path.StartPath(p.qs, p.Identifier).Out(quad.IRI(rdfs.Range).Full())
	it := rangePath.BuildIterator(p.ctx).Iterate()
	for it.Next(p.ctx) {
		ref := it.Result()
		value := p.qs.NameOf(ref)
		return value, nil
	}
	return nil, fmt.Errorf("No range was defined for property %v", p)
}
