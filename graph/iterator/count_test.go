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
	it := NewCount(fixed, nil)
	require.True(t, it.Next(ctx))
	require.Equal(t, graph.PreFetched(quad.Int(5)), it.Result())
	require.False(t, it.Next(ctx))
	require.True(t, it.Contains(ctx, graph.PreFetched(quad.Int(5))))
	require.False(t, it.Contains(ctx, graph.PreFetched(quad.Int(3))))

	fixed.Reset()

	fixed2 := NewFixed(
		graph.PreFetched(quad.String("b")),
		graph.PreFetched(quad.String("d")),
	)
	it = NewCount(NewAnd(fixed, fixed2), nil)
	require.True(t, it.Next(ctx))
	require.Equal(t, graph.PreFetched(quad.Int(2)), it.Result())
	require.False(t, it.Next(ctx))
	require.False(t, it.Contains(ctx, graph.PreFetched(quad.Int(5))))
	require.True(t, it.Contains(ctx, graph.PreFetched(quad.Int(2))))
}
