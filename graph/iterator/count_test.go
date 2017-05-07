package iterator

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCount(t *testing.T) {
	fixed := NewFixed(Identity,
		graph.PreFetched(quad.String("a")),
		graph.PreFetched(quad.String("b")),
		graph.PreFetched(quad.String("c")),
		graph.PreFetched(quad.String("d")),
		graph.PreFetched(quad.String("e")),
	)
	it := NewCount(fixed, nil)
	require.True(t, it.Next())
	require.Equal(t, graph.PreFetched(quad.Int(5)), it.Result())
	require.False(t, it.Next())
	require.True(t, it.Contains(graph.PreFetched(quad.Int(5))))
	require.False(t, it.Contains(graph.PreFetched(quad.Int(3))))

	fixed.Reset()

	fixed2 := NewFixed(Identity,
		graph.PreFetched(quad.String("b")),
		graph.PreFetched(quad.String("d")),
	)
	it = NewCount(NewAnd(nil, fixed, fixed2), nil)
	require.True(t, it.Next())
	require.Equal(t, graph.PreFetched(quad.Int(2)), it.Result())
	require.False(t, it.Next())
	require.False(t, it.Contains(graph.PreFetched(quad.Int(5))))
	require.True(t, it.Contains(graph.PreFetched(quad.Int(2))))

	it.Reset()
	it.Tagger().Add("count")
	require.True(t, it.Next())
	m := make(map[string]graph.Value)
	it.TagResults(m)
	require.Equal(t, map[string]graph.Value{"count": graph.PreFetched(quad.Int(2))}, m)
}
