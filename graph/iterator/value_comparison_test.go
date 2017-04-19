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
	"errors"
	"reflect"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
)

var (
	simpleStore = &oldstore{data: []string{"0", "1", "2", "3", "4", "5"}, parse: true}
	stringStore = &oldstore{data: []string{"foo", "bar", "baz", "echo"}, parse: true}
	mixedStore  = &oldstore{data: []string{"0", "1", "2", "3", "4", "5", "foo", "bar", "baz", "echo"}, parse: true}
)

func simpleFixedIterator() *Fixed {
	f := NewFixed(Identity)
	for i := 0; i < 5; i++ {
		f.Add(Int64Node(i))
	}
	return f
}

func stringFixedIterator() *Fixed {
	f := NewFixed(Identity)
	for _, value := range stringStore.data {
		f.Add(stringNode(value))
	}
	return f
}

func mixedFixedIterator() *Fixed {
	f := NewFixed(Identity)
	for i := 0; i < len(mixedStore.data); i++ {
		f.Add(Int64Node(i))
	}
	return f
}

type stringNode string

func (stringNode) IsNode() bool { return true }

var comparisonTests = []struct {
	message  string
	operand  quad.Value
	operator Operator
	expect   []quad.Value
	qs       graph.QuadStore
	iterator func() *Fixed
}{
	{
		message:  "successful int64 less than comparison",
		operand:  quad.Int(3),
		operator: CompareLT,
		expect:   []quad.Value{quad.Int(0), quad.Int(1), quad.Int(2)},
		qs:       simpleStore,
		iterator: simpleFixedIterator,
	},
	{
		message:  "empty int64 less than comparison",
		operand:  quad.Int(0),
		operator: CompareLT,
		expect:   nil,
		qs:       simpleStore,
		iterator: simpleFixedIterator,
	},
	{
		message:  "successful int64 greater than comparison",
		operand:  quad.Int(2),
		operator: CompareGT,
		expect:   []quad.Value{quad.Int(3), quad.Int(4)},
		qs:       simpleStore,
		iterator: simpleFixedIterator,
	},
	{
		message:  "successful int64 greater than or equal comparison",
		operand:  quad.Int(2),
		operator: CompareGTE,
		expect:   []quad.Value{quad.Int(2), quad.Int(3), quad.Int(4)},
		qs:       simpleStore,
		iterator: simpleFixedIterator,
	},
	{
		message:  "successful int64 greater than or equal comparison (mixed)",
		operand:  quad.Int(2),
		operator: CompareGTE,
		expect:   []quad.Value{quad.Int(2), quad.Int(3), quad.Int(4), quad.Int(5)},
		qs:       mixedStore,
		iterator: mixedFixedIterator,
	},
	{
		message:  "successful string less than comparison",
		operand:  quad.String("echo"),
		operator: CompareLT,
		expect:   []quad.Value{quad.String("bar"), quad.String("baz")},
		qs:       stringStore,
		iterator: stringFixedIterator,
	},
	{
		message:  "empty string less than comparison",
		operand:  quad.String(""),
		operator: CompareLT,
		expect:   nil,
		qs:       stringStore,
		iterator: stringFixedIterator,
	},
	{
		message:  "successful string greater than comparison",
		operand:  quad.String("echo"),
		operator: CompareGT,
		expect:   []quad.Value{quad.String("foo")},
		qs:       stringStore,
		iterator: stringFixedIterator,
	},
	{
		message:  "successful string greater than or equal comparison",
		operand:  quad.String("echo"),
		operator: CompareGTE,
		expect:   []quad.Value{quad.String("foo"), quad.String("echo")},
		qs:       stringStore,
		iterator: stringFixedIterator,
	},
}

func TestValueComparison(t *testing.T) {
	for _, test := range comparisonTests {
		qs := test.qs
		vc := NewComparison(test.iterator(), test.operator, test.operand, qs)

		var got []quad.Value
		for vc.Next() {
			got = append(got, qs.NameOf(vc.Result()))
		}
		if !reflect.DeepEqual(got, test.expect) {
			t.Errorf("Failed to show %s, got:%q expect:%q", test.message, got, test.expect)
		}
	}
}

var vciContainsTests = []struct {
	message  string
	operator Operator
	check    graph.Value
	expect   bool
	qs       graph.QuadStore
	val      quad.Value
	iterator func() *Fixed
}{
	{
		message:  "1 is less than 2",
		operator: CompareGTE,
		check:    Int64Node(1),
		expect:   false,
		qs:       simpleStore,
		val:      quad.Int(2),
		iterator: simpleFixedIterator,
	},
	{
		message:  "2 is greater than or equal to 2",
		operator: CompareGTE,
		check:    Int64Node(2),
		expect:   true,
		qs:       simpleStore,
		val:      quad.Int(2),
		iterator: simpleFixedIterator,
	},
	{
		message:  "3 is greater than or equal to 2",
		operator: CompareGTE,
		check:    Int64Node(3),
		expect:   true,
		qs:       simpleStore,
		val:      quad.Int(2),
		iterator: simpleFixedIterator,
	},
	{
		message:  "5 is absent from iterator",
		operator: CompareGTE,
		check:    Int64Node(5),
		expect:   false,
		qs:       simpleStore,
		val:      quad.Int(2),
		iterator: simpleFixedIterator,
	},
	{
		message:  "foo is greater than or equal to echo",
		operator: CompareGTE,
		check:    stringNode("foo"),
		expect:   true,
		qs:       stringStore,
		val:      quad.String("echo"),
		iterator: stringFixedIterator,
	},
	{
		message:  "echo is greater than or equal to echo",
		operator: CompareGTE,
		check:    stringNode("echo"),
		expect:   true,
		qs:       stringStore,
		val:      quad.String("echo"),
		iterator: stringFixedIterator,
	},
	{
		message:  "foo is missing from the iterator",
		operator: CompareLTE,
		check:    stringNode("foo"),
		expect:   false,
		qs:       stringStore,
		val:      quad.String("echo"),
		iterator: stringFixedIterator,
	},
}

func TestVCIContains(t *testing.T) {
	for _, test := range vciContainsTests {
		vc := NewComparison(test.iterator(), test.operator, test.val, test.qs)
		if vc.Contains(test.check) != test.expect {
			t.Errorf("Failed to show %s", test.message)
		}
	}
}

var comparisonIteratorTests = []struct {
	message string
	qs      graph.QuadStore
	val     quad.Value
}{
	{
		message: "2 is absent from iterator",
		qs:      simpleStore,
		val:     quad.Int(2),
	},
	{
		message: "'missing' is absent from iterator",
		qs:      stringStore,
		val:     quad.String("missing"),
	},
}

func TestComparisonIteratorErr(t *testing.T) {
	wantErr := errors.New("unique")
	errIt := newTestIterator(false, wantErr)

	for _, test := range comparisonIteratorTests {
		vc := NewComparison(errIt, CompareLT, test.val, test.qs)

		if vc.Next() != false {
			t.Errorf("Comparison iterator did not pass through initial 'false': %s", test.message)
		}
		if vc.Err() != wantErr {
			t.Errorf("Comparison iterator did not pass through underlying Err: %s", test.message)
		}
	}
}
