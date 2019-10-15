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
)

func TestMaterializeIteratorError(t *testing.T) {
	ctx := context.TODO()
	wantErr := errors.New("unique")
	errIt := newTestIterator(false, wantErr)

	// This tests that we properly return 0 results and the error when the
	// underlying iterator returns an error.
	mIt := NewMaterialize(errIt).Iterate()

	require.False(t, mIt.Next(ctx))
	require.Equal(t, wantErr, mIt.Err())
}

func TestMaterializeIteratorErrorAbort(t *testing.T) {
	ctx := context.TODO()
	wantErr := errors.New("unique")
	errIt := newTestIterator(false, wantErr)

	// This tests that we properly return 0 results and the error when the
	// underlying iterator is larger than our 'abort at' value, and then
	// returns an error.
	or := NewOr(
		newInt64(1, int64(MaterializeLimit+1), true),
		errIt,
	)

	mIt := NewMaterialize(or).Iterate()

	// We should get all the underlying values...
	for i := 0; i < MaterializeLimit+1; i++ {
		require.True(t, mIt.Next(ctx))
		require.NoError(t, mIt.Err())
	}

	// ... and then the error value.
	require.False(t, mIt.Next(ctx))
	require.Equal(t, wantErr, mIt.Err())
}
