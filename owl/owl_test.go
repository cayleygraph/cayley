package owl

import (
	"context"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc/rdf"
	"github.com/cayleygraph/quad/voc/rdfs"
	"github.com/stretchr/testify/require"
)

var fooID = quad.IRI("ex:Foo").Full()
var barID = quad.IRI("ex:Bar").Full()
var bazID = quad.IRI("ex:baz").Full()
var fooBazCardinalityRestriction = quad.RandomBlankNode()
var barBazMaxCardinalityRestriction = quad.RandomBlankNode()
var exampleGraph = quad.IRI("ex:graph")
var testSet = []quad.Quad{
	quad.Quad{
		Subject:   fooID,
		Predicate: quad.IRI(rdf.Type).Full(),
		Object:    quad.IRI(rdfs.Class).Full(),
		Label:     exampleGraph,
	},
	quad.Quad{
		Subject:   fooID,
		Predicate: quad.IRI(rdfs.SubClassOf).Full(),
		Object:    fooBazCardinalityRestriction,
		Label:     exampleGraph,
	},
	quad.Quad{
		Subject:   fooBazCardinalityRestriction,
		Predicate: quad.IRI(rdf.Type).Full(),
		Object:    quad.IRI(Restriction),
		Label:     exampleGraph,
	},
	quad.Quad{
		Subject:   fooBazCardinalityRestriction,
		Predicate: quad.IRI(OnProperty),
		Object:    bazID,
		Label:     exampleGraph,
	},
	quad.Quad{
		Subject:   fooBazCardinalityRestriction,
		Predicate: quad.IRI(Cardinality),
		Object:    quad.Int(1),
		Label:     exampleGraph,
	},
	quad.Quad{
		Subject:   barBazMaxCardinalityRestriction,
		Predicate: quad.IRI(rdf.Type).Full(),
		Object:    quad.IRI(Restriction),
		Label:     exampleGraph,
	},
	quad.Quad{
		Subject:   barBazMaxCardinalityRestriction,
		Predicate: quad.IRI(OnProperty),
		Object:    bazID,
		Label:     exampleGraph,
	},
	quad.Quad{
		Subject:   barBazMaxCardinalityRestriction,
		Predicate: quad.IRI(MaxCardinality),
		Object:    quad.Int(1),
		Label:     exampleGraph,
	},
	quad.Quad{
		Subject:   barID,
		Predicate: quad.IRI(rdfs.SubClassOf).Full(),
		Object:    fooID,
		Label:     exampleGraph,
	},
	quad.Quad{
		Subject:   barID,
		Predicate: quad.IRI(rdfs.SubClassOf).Full(),
		Object:    barBazMaxCardinalityRestriction,
		Label:     exampleGraph,
	},
	quad.Quad{
		Subject:   bazID,
		Predicate: quad.IRI(rdfs.Domain).Full(),
		Object:    fooID,
		Label:     exampleGraph,
	},
	quad.Quad{
		Subject:   bazID,
		Predicate: quad.IRI(rdfs.Range).Full(),
		Object:    barID,
		Label:     exampleGraph,
	},
}

func TestGetClass(t *testing.T) {
	ctx := context.TODO()
	qs := memstore.New(testSet...)
	class, err := GetClass(ctx, qs, fooID)
	require.NoError(t, err)
	require.Equal(t, class.Identifier, fooID)
}

func TestSubClasses(t *testing.T) {
	ctx := context.TODO()
	qs := memstore.New(testSet...)
	fooClass, err := GetClass(ctx, qs, fooID)
	require.NoError(t, err)
	barClass, err := GetClass(ctx, qs, barID)
	require.NoError(t, err)
	subClasses := fooClass.SubClasses()
	require.Len(t, subClasses, 1)
	require.Contains(t, subClasses, barClass)
}

func TestProperties(t *testing.T) {
	ctx := context.TODO()
	qs := memstore.New(testSet...)
	fooClass, err := GetClass(ctx, qs, fooID)
	require.NoError(t, err)
	bazProperty, err := GetProperty(ctx, qs, bazID)
	require.NoError(t, err)
	properties := fooClass.Properties()
	require.Len(t, properties, 1)
	require.Contains(t, properties, bazProperty)
}

func TestParentClasses(t *testing.T) {
	ctx := context.TODO()
	qs := memstore.New(testSet...)
	fooClass, err := GetClass(ctx, qs, fooID)
	require.NoError(t, err)
	bazProperty, err := GetProperty(ctx, qs, bazID)
	require.NoError(t, err)
	properties := fooClass.Properties()
	require.Len(t, properties, 1)
	require.Contains(t, properties, bazProperty)
}

func TestCardinalityOf(t *testing.T) {
	ctx := context.TODO()
	qs := memstore.New(testSet...)
	fooClass, err := GetClass(ctx, qs, fooID)
	require.NoError(t, err)
	bazProperty, err := GetProperty(ctx, qs, bazID)
	require.NoError(t, err)
	cardinality, err := fooClass.CardinalityOf(bazProperty)
	require.NoError(t, err)
	require.Equal(t, cardinality, int64(1))
}

func TestMaxCardinalityOf(t *testing.T) {
	ctx := context.TODO()
	qs := memstore.New(testSet...)
	fooClass, err := GetClass(ctx, qs, barID)
	require.NoError(t, err)
	bazProperty, err := GetProperty(ctx, qs, bazID)
	require.NoError(t, err)
	cardinality, err := fooClass.MaxCardinalityOf(bazProperty)
	require.NoError(t, err)
	require.Equal(t, cardinality, int64(1))
}

func TestRange(t *testing.T) {
	ctx := context.TODO()
	qs := memstore.New(testSet...)
	bazProperty, err := GetProperty(ctx, qs, bazID)
	require.NoError(t, err)
	_range, err := bazProperty.Range()
	require.NoError(t, err)
	require.Equal(t, _range, barID)
}

func collectPath(ctx context.Context, qs graph.QuadStore, p *path.Path) []quad.Value {
	var values []quad.Value
	it := p.BuildIterator(ctx).Iterate()
	for it.Next(ctx) {
		ref := it.Result()
		value := qs.NameOf(ref)
		values = append(values, value)
	}
	return values
}

func TestParentClassesPath(t *testing.T) {
	ctx := context.TODO()
	qs := memstore.New(testSet...)
	fooClass, err := GetClass(ctx, qs, fooID)
	require.NoError(t, err)
	p := parentClassesPath(fooClass)
	values := collectPath(ctx, qs, p)
	require.Equal(t, []quad.Value{
		fooBazCardinalityRestriction,
	}, values)
}

func TestRestrictionsPath(t *testing.T) {
	ctx := context.TODO()
	qs := memstore.New(testSet...)
	fooClass, err := GetClass(ctx, qs, fooID)
	require.NoError(t, err)
	p := restrictionsPath(fooClass)
	values := collectPath(ctx, qs, p)
	require.Equal(t, []quad.Value{
		fooBazCardinalityRestriction,
	}, values)
}
