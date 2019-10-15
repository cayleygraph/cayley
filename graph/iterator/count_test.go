package iterator

import (
	"context"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"
	"github.com/stretchr/testify/require"
)

func TestCount(t *testing.T) {
	ctx := context.TODO()
	fixed := NewFixed(
		graph.PreFetched(quad.String("a")),
		graph.PreFetched(quad.String("b")),
		graph.PreFetched(quad.String("c")),
		graph.PreFetched(quad.String("d")),
		graph.PreFetched(quad.String("e")),
	)
	its := NewCount(fixed, nil)

	itn := its.Iterate()
	require.True(t, itn.Next(ctx))
	require.Equal(t, graph.PreFetched(quad.Int(5)), itn.Result())
	require.False(t, itn.Next(ctx))

	itc := its.Lookup()
	require.True(t, itc.Contains(ctx, graph.PreFetched(quad.Int(5))))
	require.False(t, itc.Contains(ctx, graph.PreFetched(quad.Int(3))))

	fixed2 := NewFixed(
		graph.PreFetched(quad.String("b")),
		graph.PreFetched(quad.String("d")),
	)
	its = NewCount(NewAnd(fixed, fixed2), nil)

	itn = its.Iterate()
	require.True(t, itn.Next(ctx))
	require.Equal(t, graph.PreFetched(quad.Int(2)), itn.Result())
	require.False(t, itn.Next(ctx))

	itc = its.Lookup()
	require.False(t, itc.Contains(ctx, graph.PreFetched(quad.Int(5))))
	require.True(t, itc.Contains(ctx, graph.PreFetched(quad.Int(2))))
}
