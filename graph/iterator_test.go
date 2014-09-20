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

package graph

import (
	"testing"
)

func TestTypeEncodingRoundtrip(t *testing.T) {
	for i := Invalid; i <= Materialize; i++ {
		text, err := i.MarshalText()
		if err != nil {
			t.Errorf("Unexpected error when marshaling %s: %v", i, err)
		}
		if string(text) != i.String() {
			t.Errorf("Unexpected MarshalText result, got:%q expect:%q", i, text)
		}
		var m Type
		err = m.UnmarshalText(text)
		if i == Invalid {
			if err == nil || err.Error() != `graph: unknown iterator label: "invalid"` {
				t.Errorf("Unexpected error when unmarshaling %q: %v", text, err)
			}
		} else {
			if err != nil {
				t.Errorf("Unexpected error when unmarshaling %q: %v", text, err)
			}
		}
		if m != i {
			t.Errorf("Unexpected UnmarshalText result, got:Type(%d) expect:Type(%d)", m, i)
		}
	}
}
