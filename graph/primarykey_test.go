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

	. "github.com/google/cayley/graph"
	"github.com/pborman/uuid"
)

func TestSequentialKeyCreation(t *testing.T) {
	{
		seqKey := NewSequentialKey(666)
		seqKey.Next()

		var expected int64 = 667
		result := seqKey.Int()
		if expected != result {
			t.Errorf("Expected %q got %q", expected, result)
		}
	}
	{
		seqKey := NewSequentialKey(0)
		seqKey.Next()

		var expected int64 = 1
		result := seqKey.Int()
		if expected != result {
			t.Errorf("Expected %q got %q", expected, result)
		}
	}
}

func TestUniqueKeyCreation(t *testing.T) {
	uniqueKey := NewUniqueKey("")
	if uuid.Parse(uniqueKey.String()) == nil {
		t.Error("Invalid uuid generated")
	}
	uniqueKey.Next()
	if uuid.Parse(uniqueKey.String()) == nil {
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
	if seqKey.Int() != seqKey2.Int() {
		t.Errorf("Unmarshaling failed: Expected: %d, got: %d", seqKey.Int(), seqKey2.Int())
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
	if uuid.Parse(uniqueKey2.String()) == nil {
		t.Error("Unique Key incorrectly unmarshaled")
	}
	if uniqueKey.String() != uniqueKey2.String() {
		t.Error("Unique Key incorrectly unmarshaled")
	}
}
