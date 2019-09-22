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

package sexp

import (
	"context"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"

	"github.com/cayleygraph/cayley/graph/graphtest/testutil"
	_ "github.com/cayleygraph/cayley/graph/memstore"
	sh "github.com/cayleygraph/cayley/graph/shape"
	_ "github.com/cayleygraph/cayley/writer"
	"github.com/stretchr/testify/require"
)

func TestBadParse(t *testing.T) {
	str := ParseString("()")
	if str != "" {
		t.Errorf("Unexpected parse result, got:%q", str)
	}
}

var (
	quads1 = []quad.Quad{quad.Make("i", "can", "win", nil)}
)

var testQueries = []struct {
	message string
	add     []quad.Quad
	query   string
	shape   sh.Shape
	expect  string
	tags    map[string]string
}{
	{
		message: "empty",
		query:   "()",
		shape:   sh.Null{},
	},
	{
		message: "get a single quad linkage",
		add:     quads1,
		query:   "($a (:can \"win\"))",
		shape: sh.Save{
			Tags: []string{"$a"},
			From: sh.NodesFrom{
				Dir: quad.Subject,
				Quads: sh.Quads{
					{Dir: quad.Predicate, Values: lookup("can")},
					{Dir: quad.Object, Values: lookup("win")},
				},
			},
		},
		expect: "i",
	},
	{
		message: "get a single quad linkage (internal)",
		add:     quads1,
		query:   "(\"i\" (:can $a))",
		shape: sh.Intersect{
			lookup("i"),
			sh.NodesFrom{
				Dir: quad.Subject,
				Quads: sh.Quads{
					{Dir: quad.Predicate, Values: lookup("can")},
					{
						Dir: quad.Object, Values: sh.Save{
							Tags: []string{"$a"},
							From: sh.AllNodes{},
						},
					},
				},
			},
		},
		expect: "i",
	},
	{
		message: "tree constraint",
		add: []quad.Quad{
			quad.Make("i", "like", "food", nil),
			quad.Make("food", "is", "good", nil),
		},
		query: "(\"i\"\n" +
			"(:like\n" +
			"($a (:is :good))))",
		shape: sh.Intersect{
			lookup("i"),
			sh.NodesFrom{
				Dir: quad.Subject,
				Quads: sh.Quads{
					{Dir: quad.Predicate, Values: lookup("like")},
					{
						Dir: quad.Object, Values: sh.Save{
							Tags: []string{"$a"},
							From: sh.NodesFrom{
								Dir: quad.Subject,
								Quads: sh.Quads{
									{Dir: quad.Predicate, Values: lookup("is")},
									{Dir: quad.Object, Values: lookup("good")},
								},
							},
						},
					},
				},
			},
		},
		expect: "i",
		tags: map[string]string{
			"$a": "food",
		},
	},
	{
		message: "multiple constraint",
		add: []quad.Quad{
			quad.Make("i", "like", "food", nil),
			quad.Make("i", "like", "beer", nil),
			quad.Make("you", "like", "beer", nil),
		},
		query: `(
			$a
			(:like :beer)
			(:like "food")
		)`,
		shape: sh.Save{
			Tags: []string{"$a"},
			From: sh.Intersect{
				sh.NodesFrom{
					Dir: quad.Subject,
					Quads: sh.Quads{
						{Dir: quad.Predicate, Values: lookup("like")},
						{Dir: quad.Object, Values: lookup("beer")},
					},
				},
				sh.NodesFrom{
					Dir: quad.Subject,
					Quads: sh.Quads{
						{Dir: quad.Predicate, Values: lookup("like")},
						{Dir: quad.Object, Values: lookup("food")},
					},
				},
			},
		},
		expect: "i",
	},
}

func TestSexp(t *testing.T) {
	ctx := context.TODO()
	for _, test := range testQueries {
		t.Run(test.message, func(t *testing.T) {
			qs, _ := graph.NewQuadStore("memstore", "", nil)
			_ = testutil.MakeWriter(t, qs, nil, test.add...)

			s, _ := BuildShape(test.query)
			require.Equal(t, test.shape, s, "%s\n%#v\nvs\n%#v", test.message, test.shape, s)

			it := BuildIteratorTreeForQuery(qs, test.query)
			if it.Next(ctx) != (test.expect != "") {
				t.Errorf("Failed to %s", test.message)
			}
			if test.expect != "" {
				require.Equal(t, qs.ValueOf(quad.StringToValue(test.expect)), it.Result())

				tags := make(map[string]graph.Ref)
				it.TagResults(tags)
				for k, v := range test.tags {
					name := qs.NameOf(tags[k])
					require.Equal(t, v, quad.ToString(name))
				}
				if it.Next(ctx) {
					t.Error("too many results")
				}
			}
		})
	}
}
