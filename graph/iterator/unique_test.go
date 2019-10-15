package iterator_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	. "github.com/cayleygraph/cayley/graph/iterator"
)

func TestUniqueIteratorBasics(t *testing.T) {
	ctx := context.TODO()
	allIt := NewFixed(
		Int64Node(1),
		Int64Node(2),
		Int64Node(3),
		Int64Node(3),
		Int64Node(2),
	)

	u := NewUnique(allIt)

	expect := []int{1, 2, 3}
	for i := 0; i < 2; i++ {
		require.Equal(t, expect, iterated(u))
	}

	uc := u.Lookup()
	for _, v := range []int{1, 2, 3} {
		require.True(t, uc.Contains(ctx, Int64Node(v)))
	}
}
