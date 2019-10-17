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

package graph_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/quad"
)

func TestHasAIteratorErr(t *testing.T) {
	wantErr := errors.New("unique")
	ctx := context.TODO()
	errIt := iterator.NewError(wantErr)

	// TODO(andrew-d): pass a non-nil quadstore
	hasa := graph.NewHasA(nil, errIt, quad.Subject).Iterate()

	require.False(t, hasa.Next(ctx), "HasA iterator did not pass through initial 'false'")
	require.Equal(t, wantErr, hasa.Err(), "HasA iterator did not pass through underlying Err")
}
