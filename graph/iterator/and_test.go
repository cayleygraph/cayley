// Copyright 2014 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iterator_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	. "github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/refs"
)

// Make sure that tags work on the And.
func TestAndTag(t *testing.T) {
	ctx := context.TODO()
	fix1 := NewFixed(Int64Node(234))
	fix2 := NewFixed(Int64Node(234))
	var ands Shape = NewAnd(Tag(fix1, "foo")).AddOptionalIterator(Tag(fix2, "baz"))
	ands = Tag(ands, "bar")

	and := ands.Iterate()
	require.True(t, and.Next(ctx))
	require.Equal(t, Int64Node(234), and.Result())

	tags := make(map[string]refs.Ref)
	and.TagResults(tags)
	require.Equal(t, map[string]refs.Ref{
		"foo": Int64Node(234),
		"bar": Int64Node(234),
		"baz": Int64Node(234),
	}, tags)
}

// Do a simple itersection of fixed values.
func TestAndAndFixedIterators(t *testing.T) {
	ctx := context.TODO()
	fix1 := NewFixed(
		Int64Node(1),
		Int64Node(2),
		Int64Node(3),
		Int64Node(4),
	)
	fix2 := NewFixed(
		Int64Node(3),
		Int64Node(4),
		Int64Node(5),
	)
	ands := NewAnd(fix1, fix2)
	// Should be as big as smallest subiterator
	st, err := ands.Stats(ctx)
	require.NoError(t, err)
	require.Equal(t, refs.Size{
		Value: 3,
		Exact: true,
	}, st.Size)

	and := ands.Iterate()

	require.True(t, and.Next(ctx))
	require.Equal(t, Int64Node(3), and.Result())

	require.True(t, and.Next(ctx))
	require.Equal(t, Int64Node(4), and.Result())

	require.False(t, and.Next(ctx))
}

// If there's no intersection, the size should still report the same,
// but there should be nothing to Next()
func TestNonOverlappingFixedIterators(t *testing.T) {
	ctx := context.TODO()
	fix1 := NewFixed(
		Int64Node(1),
		Int64Node(2),
		Int64Node(3),
		Int64Node(4),
	)
	fix2 := NewFixed(
		Int64Node(5),
		Int64Node(6),
		Int64Node(7),
	)
	ands := NewAnd(fix1, fix2)
	// Should be as big as smallest subiterator
	st, err := ands.Stats(ctx)
	require.NoError(t, err)
	require.Equal(t, refs.Size{
		Value: 3,
		Exact: true,
	}, st.Size)

	and := ands.Iterate()
	require.False(t, and.Next(ctx))
}

func TestAllIterators(t *testing.T) {
	ctx := context.TODO()
	all1 := newInt64(1, 5, true)
	all2 := newInt64(4, 10, true)
	and := NewAnd(all2, all1).Iterate()

	require.True(t, and.Next(ctx))
	require.Equal(t, Int64Node(4), and.Result())

	require.True(t, and.Next(ctx))
	require.Equal(t, Int64Node(5), and.Result())

	require.False(t, and.Next(ctx))
}

func TestAndIteratorErr(t *testing.T) {
	ctx := context.TODO()
	wantErr := errors.New("unique")
	allErr := newTestIterator(false, wantErr)

	and := NewAnd(
		allErr,
		newInt64(1, 5, true),
	).Iterate()

	require.False(t, and.Next(ctx))
	require.Equal(t, wantErr, and.Err())
}
