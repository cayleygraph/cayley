package owl

import (
	"context"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc/owl"
	"github.com/cayleygraph/quad/voc/rdf"
	"github.com/cayleygraph/quad/voc/rdfs"
	"github.com/stretchr/testify/require"
)

var (
	fooID                           = quad.IRI("ex:Foo").Full()
	barID                           = quad.IRI("ex:Bar").Full()
	garID                           = quad.IRI("ex:Gar").Full()
	bazID                           = quad.IRI("ex:baz").Full()
	fooBarGarUnion                  = quad.RandomBlankNode()
	fooBazCardinalityRestriction    = quad.RandomBlankNode()
	barBazMaxCardinalityRestriction = quad.RandomBlankNode()
	exampleGraph                    = quad.IRI("ex:graph")
)
var fooClassQuads = []quad.Quad{
	{
		Subject:   fooID,
		Predicate: quad.IRI(rdf.Type).Full(),
		Object:    quad.IRI(rdfs.Class).Full(),
		Label:     exampleGraph,
	},
}
var bazPropertyQuads = []quad.Quad{
	{
		Subject:   barID,
		Predicate: quad.IRI(rdfs.SubClassOf).Full(),
		Object:    fooID,
		Label:     exampleGraph,
	},

	{
		Subject:   bazID,
		Predicate: quad.IRI(rdfs.Domain).Full(),
		Object:    fooID,
		Label:     exampleGraph,
	},
	{
		Subject:   bazID,
		Predicate: quad.IRI(rdfs.Range).Full(),
		Object:    barID,
		Label:     exampleGraph,
	},
}
var fooBazCardinalityRestrictionQuads = []quad.Quad{
	{
		Subject:   fooBazCardinalityRestriction,
		Predicate: quad.IRI(rdf.Type).Full(),
		Object:    quad.IRI(owl.Restriction),
		Label:     exampleGraph,
	},
	{
		Subject:   fooBazCardinalityRestriction,
		Predicate: quad.IRI(owl.OnProperty),
		Object:    bazID,
		Label:     exampleGraph,
	},
	{
		Subject:   fooBazCardinalityRestriction,
		Predicate: quad.IRI(owl.Cardinality),
		Object:    quad.Int(1),
		Label:     exampleGraph,
	},
	{
		Subject:   fooID,
		Predicate: quad.IRI(rdfs.SubClassOf).Full(),
		Object:    fooBazCardinalityRestriction,
		Label:     exampleGraph,
	},
}
var barBazCardinalityRestrictionQuad = []quad.Quad{
	{
		Subject:   barBazMaxCardinalityRestriction,
		Predicate: quad.IRI(rdf.Type).Full(),
		Object:    quad.IRI(owl.Restriction),
		Label:     exampleGraph,
	},
	{
		Subject:   barBazMaxCardinalityRestriction,
		Predicate: quad.IRI(owl.OnProperty),
		Object:    bazID,
		Label:     exampleGraph,
	},
	{
		Subject:   barBazMaxCardinalityRestriction,
		Predicate: quad.IRI(owl.MaxCardinality),
		Object:    quad.Int(1),
		Label:     exampleGraph,
	},
	{
		Subject:   barID,
		Predicate: quad.IRI(rdfs.SubClassOf).Full(),
		Object:    barBazMaxCardinalityRestriction,
		Label:     exampleGraph,
	},
}

func listQuads(items []quad.Value, label quad.Value) (quad.Value, []quad.Quad) {
	var quads []quad.Quad
	list := quad.RandomBlankNode()
	cursor := list
	for i, item := range items {
		first := quad.Quad{
			Subject:   cursor,
			Predicate: quad.IRI(rdf.First).Full(),
			Object:    item,
			Label:     label,
		}
		var rest quad.Quad
		if i < len(items)-1 {
			rest = quad.Quad{
				Subject:   cursor,
				Predicate: quad.IRI(rdf.Rest).Full(),
				Object:    quad.IRI(rdf.Nil).Full(),
				Label:     label,
			}
		} else {
			nextCursor := quad.RandomBlankNode()
			rest = quad.Quad{
				Subject:   cursor,
				Predicate: quad.IRI(rdf.Rest).Full(),
				Object:    nextCursor,
				Label:     label,
			}
			cursor = nextCursor
		}
		quads = append(quads, first, rest)
	}
	return list, quads
}

func getUnionQuads() []quad.Quad {
	var unionQuads []quad.Quad
	membersList, membersQuads := listQuads(
		[]quad.Value{fooID, barID, garID},
		exampleGraph,
	)
	unionQuads = append(unionQuads, membersQuads...)
	unionQuads = append(unionQuads, quad.Quad{
		Subject:   fooBarGarUnion,
		Predicate: quad.IRI(owl.UnionOf),
		Object:    membersList,
		Label:     exampleGraph,
	})
	return unionQuads
}

func getTestSet() []quad.Quad {
	var testSet []quad.Quad
	testSet = append(testSet, fooBazCardinalityRestrictionQuads...)
	testSet = append(testSet, barBazCardinalityRestrictionQuad...)
	testSet = append(testSet, bazPropertyQuads...)
	testSet = append(testSet, getUnionQuads()...)
	return testSet
}

func TestListContainingPath(t *testing.T) {
	ctx := context.TODO()
	qs := memstore.New(getTestSet()...)
	p := listContainignPath(qs, fooID).In(quad.IRI(owl.UnionOf))
	values := collectPath(ctx, qs, p)
	require.Equal(t, []quad.Value{
		fooBarGarUnion,
	}, values)
	p = listContainignPath(qs, barID).In(quad.IRI(owl.UnionOf))
	values = collectPath(ctx, qs, p)
	require.Equal(t, []quad.Value{
		fooBarGarUnion,
	}, values)
	p = listContainignPath(qs, garID).In(quad.IRI(owl.UnionOf))
	values = collectPath(ctx, qs, p)
	require.Equal(t, []quad.Value{
		fooBarGarUnion,
	}, values)
	p = listContainignPath(qs, bazID).In(quad.IRI(owl.UnionOf))
	values = collectPath(ctx, qs, p)
	require.Equal(t, []quad.Value(nil), values)
}

func TestGetClass(t *testing.T) {
	ctx := context.TODO()
	qs := memstore.New(getTestSet()...)
	class, err := GetClass(ctx, qs, fooID)
	require.NoError(t, err)
	require.Equal(t, class.Identifier, fooID)
}

func TestSubClasses(t *testing.T) {
	ctx := context.TODO()
	qs := memstore.New(getTestSet()...)
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
	qs := memstore.New(getTestSet()...)
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
	qs := memstore.New(getTestSet()...)
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
	qs := memstore.New(getTestSet()...)
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
	qs := memstore.New(getTestSet()...)
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
	qs := memstore.New(getTestSet()...)
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
	qs := memstore.New(getTestSet()...)
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
	qs := memstore.New(getTestSet()...)
	fooClass, err := GetClass(ctx, qs, fooID)
	require.NoError(t, err)
	p := restrictionsPath(fooClass)
	values := collectPath(ctx, qs, p)
	require.Equal(t, []quad.Value{
		fooBazCardinalityRestriction,
	}, values)
}
