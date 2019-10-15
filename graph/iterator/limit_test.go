package iterator_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	. "github.com/cayleygraph/cayley/graph/iterator"
)

func TestLimitIteratorBasics(t *testing.T) {
	ctx := context.TODO()
	allIt := NewFixed(
		Int64Node(1),
		Int64Node(2),
		Int64Node(3),
		Int64Node(4),
		Int64Node(5),
	)

	u := NewLimit(allIt, 0)
	expectSz, _ := allIt.Stats(ctx)
	sz, _ := u.Stats(ctx)
	require.Equal(t, expectSz.Size.Value, sz.Size.Value)
	require.Equal(t, []int{1, 2, 3, 4, 5}, iterated(u))

	u = NewLimit(allIt, 3)
	sz, _ = u.Stats(ctx)
	require.Equal(t, int64(3), sz.Size.Value)
	require.Equal(t, []int{1, 2, 3}, iterated(u))

	uc := u.Lookup()
	for _, v := range []int{1, 2, 3} {
		require.True(t, uc.Contains(ctx, Int64Node(v)))
	}
	require.False(t, uc.Contains(ctx, Int64Node(4)))

	uc = u.Lookup()
	for _, v := range []int{5, 4, 3} {
		require.True(t, uc.Contains(ctx, Int64Node(v)))
	}
	require.False(t, uc.Contains(ctx, Int64Node(2)))
}
