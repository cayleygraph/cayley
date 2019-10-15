package iterator_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	. "github.com/cayleygraph/cayley/graph/iterator"
)

func TestSkipIteratorBasics(t *testing.T) {
	ctx := context.TODO()
	allIt := NewFixed(
		Int64Node(1),
		Int64Node(2),
		Int64Node(3),
		Int64Node(4),
		Int64Node(5),
	)

	u := NewSkip(allIt, 0)
	expectSz, _ := allIt.Stats(ctx)
	sz, _ := u.Stats(ctx)
	require.Equal(t, expectSz.Size.Value, sz.Size.Value)

	require.Equal(t, []int{1, 2, 3, 4, 5}, iterated(u))

	u = NewSkip(allIt, 3)
	expectSz.Size.Value = 2
	if sz, _ := u.Stats(ctx); sz.Size.Value != expectSz.Size.Value {
		t.Errorf("Failed to check Skip size: got:%v expected:%v", sz.Size, expectSz.Size)
	}
	require.Equal(t, []int{4, 5}, iterated(u))

	uc := u.Lookup()
	for _, v := range []int{1, 2, 3} {
		require.False(t, uc.Contains(ctx, Int64Node(v)))
	}
	for _, v := range []int{4, 5} {
		require.True(t, uc.Contains(ctx, Int64Node(v)))
	}

	uc = u.Lookup()
	for _, v := range []int{5, 4, 3} {
		require.False(t, uc.Contains(ctx, Int64Node(v)))
	}
	for _, v := range []int{1, 2} {
		require.True(t, uc.Contains(ctx, Int64Node(v)))
	}

	// TODO(dennwc): check with NextPath
}
