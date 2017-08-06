// Copyright 2015 The Cayley Authors. All rights reserved.
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

	. "github.com/cayleygraph/cayley/graph"
	"github.com/pborman/uuid"
)

func TestSequentialKeyCreation(t *testing.T) {
	{
		seqKey := NewSequentialKey(666)

		var expected int64 = 666
		result, _ := seqKey.Int()
		if expected != result {
			t.Errorf("Expected %q got %q", expected, result)
		}
	}
	{
		seqKey := NewSequentialKey(0)

		result, ok := seqKey.Int()
		if !ok || result != 0 {
			t.Errorf("Expected %q got %q", 0, result)
		}
	}
}

func TestUniqueKeyCreation(t *testing.T) {
	k := NewUniqueKey("")
	if s, _ := k.Unique(); uuid.Parse(s) == nil {
		t.Error("Invalid uuid generated")
	}
}

func TestSequentialKeyMarshaling(t *testing.T) {
	seqKey := NewSequentialKey(666)
	seqKeyBytes, err := seqKey.MarshalJSON()
	if err != nil {
		t.Errorf("Marshaling of sequential key failed with : %v", err)
	}

	seqKey2 := PrimaryKey{}
	err = seqKey2.UnmarshalJSON(seqKeyBytes)
	if err != nil {
		t.Errorf("Unmarshaling of sequential key failed with : %v", err)
	}
	n1, _ := seqKey.Int()
	n2, _ := seqKey2.Int()
	if seqKey != seqKey2 || n1 != n2 {
		t.Errorf("Unmarshaling failed: Expected: %d, got: %d", n1, n2)
	}
}

func TestUniqueKeyMarshaling(t *testing.T) {
	uniqueKey := NewUniqueKey("")
	uniqueKeyBytes, err := uniqueKey.MarshalJSON()
	if err != nil {
		t.Errorf("Marshaling of unique key failed with : %v", err)
	}

	uniqueKey2 := PrimaryKey{}
	err = uniqueKey2.UnmarshalJSON(uniqueKeyBytes)
	if err != nil {
		t.Errorf("Unmarshaling of unique key failed with : %v", err)
	}
	s2, _ := uniqueKey2.Unique()
	if uuid.Parse(s2) == nil {
		t.Error("Unique Key incorrectly unmarshaled")
	}
	if s1, _ := uniqueKey.Unique(); uniqueKey != uniqueKey2 || s1 != s2 {
		t.Error("Unique Key incorrectly unmarshaled")
	}
}
