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
	"testing"

	. "github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
)

func TestSingleIterator(t *testing.T) {
	all := iterator.NewInt64(1, 3)
	result := StringResultTreeEvaluator(all)
	expected := "(1)\n(2)\n(3)\n"
	if expected != result {
		t.Errorf("Expected %q got %q", expected, result)
	}
}

func TestAndIterator(t *testing.T) {
	all1 := iterator.NewInt64(1, 3)
	all2 := iterator.NewInt64(3, 5)
	and := iterator.NewAnd()
	and.AddSubIterator(all1)
	and.AddSubIterator(all2)

	result := StringResultTreeEvaluator(and)
	expected := "(3 (3) (3))\n"
	if expected != result {
		t.Errorf("Expected %q got %q", expected, result)
	}
}
