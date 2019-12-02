package owl

import (
	"context"
	"testing"

	"github.com/cayleygraph/cayley/graph/memstore"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/voc/rdf"
	"github.com/cayleygraph/quad/voc/rdfs"
	"github.com/stretchr/testify/require"
)

var fooID = quad.IRI("ex:Foo").Full()
var barID = quad.IRI("ex:Bar").Full()
var bazID = quad.IRI("ex:baz").Full()
var exampleGraph = quad.IRI("ex:graph")
var testSet = []quad.Quad{
	quad.Quad{Subject: fooID, Predicate: quad.IRI(rdf.Type).Full(), Object: quad.IRI(rdfs.SubClassOf).Full(), Label: exampleGraph},
	quad.Quad{Subject: barID, Predicate: quad.IRI(rdfs.SubClassOf).Full(), Object: fooID, Label: exampleGraph},
	quad.Quad{Subject: bazID, Predicate: quad.IRI(rdfs.Domain).Full(), Object: fooID, Label: exampleGraph},
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
