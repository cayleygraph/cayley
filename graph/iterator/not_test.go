package iterator_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	. "github.com/cayleygraph/cayley/graph/iterator"
)

func TestNotIteratorBasics(t *testing.T) {
	ctx := context.TODO()
	allIt := NewFixed(
		Int64Node(1),
		Int64Node(2),
		Int64Node(3),
		Int64Node(4),
	)

	toComplementIt := NewFixed(
		Int64Node(2),
		Int64Node(4),
	)

	not := NewNot(toComplementIt, allIt)

	st, _ := not.Stats(ctx)
	require.Equal(t, int64(2), st.Size.Value)

	expect := []int{1, 3}
	for i := 0; i < 2; i++ {
		require.Equal(t, expect, iterated(not))
	}

	nc := not.Lookup()
	for _, v := range []int{1, 3} {
		require.True(t, nc.Contains(ctx, Int64Node(v)))
	}

	for _, v := range []int{2, 4} {
		require.False(t, nc.Contains(ctx, Int64Node(v)))
	}
}

func TestNotIteratorErr(t *testing.T) {
	ctx := context.TODO()
	wantErr := errors.New("unique")
	allIt := newTestIterator(false, wantErr)

	toComplementIt := NewFixed()

	not := NewNot(toComplementIt, allIt).Iterate()

	require.False(t, not.Next(ctx))
	require.Equal(t, wantErr, not.Err())
}
