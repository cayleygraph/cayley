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

package iterator

import (
	"reflect"
	"testing"

	"github.com/google/cayley/graph"
)

var simpleStore = &store{data: []string{"0", "1", "2", "3", "4", "5"}}

func simpleFixedIterator() *Fixed {
	f := newFixed()
	for i := 0; i < 5; i++ {
		f.Add(i)
	}
	return f
}

var comparisonTests = []struct {
	message  string
	operand  graph.Value
	operator Operator
	expect   []string
}{
	{
		message:  "successful int64 less than comparison",
		operand:  int64(3),
		operator: kCompareLT,
		expect:   []string{"0", "1", "2"},
	},
	{
		message:  "empty int64 less than comparison",
		operand:  int64(0),
		operator: kCompareLT,
		expect:   nil,
	},
	{
		message:  "successful int64 greater than comparison",
		operand:  int64(2),
		operator: kCompareGT,
		expect:   []string{"3", "4"},
	},
	{
		message:  "successful int64 greater than or equal comparison",
		operand:  int64(2),
		operator: kCompareGTE,
		expect:   []string{"2", "3", "4"},
	},
}

func TestValueComparison(t *testing.T) {
	for _, test := range comparisonTests {
		ts := simpleStore
		vc := NewComparison(simpleFixedIterator(), test.operator, test.operand, ts)

		var got []string
		for {
			val, ok := vc.Next()
			if !ok {
				break
			}
			got = append(got, ts.NameOf(val))
		}
		if !reflect.DeepEqual(got, test.expect) {
			t.Errorf("Failed to show %s, got:%q expect:%q", test.message, got, test.expect)
		}
	}
}

var vciCheckTests = []struct {
	message  string
	operator Operator
	check    graph.Value
	expect   bool
}{
	{
		message:  "1 is less than 2",
		operator: kCompareGTE,
		check:    1,
		expect:   false,
	},
	{
		message:  "2 is greater than or equal to 2",
		operator: kCompareGTE,
		check:    2,
		expect:   true,
	},
	{
		message:  "3 is greater than or equal to 2",
		operator: kCompareGTE,
		check:    3,
		expect:   true,
	},
	{
		message:  "5 is absent from iterator",
		operator: kCompareGTE,
		check:    5,
		expect:   false,
	},
}

func TestVCICheck(t *testing.T) {
	for _, test := range vciCheckTests {
		vc := NewComparison(simpleFixedIterator(), test.operator, int64(2), simpleStore)
		if vc.Check(test.check) != test.expect {
			t.Errorf("Failed to show %s", test.message)
		}
	}
}
