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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphmock"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/quad"
)

func TestLinksTo(t *testing.T) {
	ctx := context.TODO()
	object := quad.Raw("cool")
	q := quad.Quad{Subject: quad.IRI("alice"), Predicate: quad.IRI("is"), Object: object, Label: nil}
	qs := &graphmock.Store{
		Data: []quad.Quad{q},
	}
	fixed := iterator.NewFixed()

	val := qs.ValueOf(object)

	fixed.Add(val)
	lto := graph.NewLinksTo(qs, fixed, quad.Object).Iterate()
	require.True(t, lto.Next(ctx))
	require.Equal(t, q, qs.Quad(lto.Result()))
}
