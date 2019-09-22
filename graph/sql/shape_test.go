package sql

import (
	"fmt"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/shape"
	"github.com/cayleygraph/quad"
	"github.com/stretchr/testify/require"
)

type stringVal string

func (s stringVal) Key() interface{} {
	return string(s)
}

func (s stringVal) SQLValue() interface{} {
	return string(s)
}

func sVal(s string) stringVal {
	return stringVal(s)
}

func sVals(arr ...string) []Value {
	out := make([]Value, 0, len(arr))
	for _, s := range arr {
		out = append(out, sVal(s))
	}
	return out
}

var shapeCases = []struct {
	skip bool
	name string
	s    shape.Shape
	qu   string
	args []Value
}{
	{
		name: "all nodes",
		s:    shape.AllNodes{},
		qu:   `SELECT hash AS ` + tagNode + ` FROM nodes`,
	},
	{
		name: "lookup iri",
		s:    shape.Lookup{quad.IRI("a")},
		qu:   `SELECT hash AS ` + tagNode + ` FROM nodes WHERE hash = $1`,
		args: []Value{HashOf(quad.IRI("a"))},
	},
	{
		name: "gt iri",
		s: shape.Filter{
			From: shape.AllNodes{},
			Filters: []shape.ValueFilter{
				shape.Comparison{Op: iterator.CompareGT, Val: quad.IRI("a")},
			},
		},
		qu:   `SELECT hash AS ` + tagNode + ` FROM nodes WHERE value_string > $1 AND iri IS true`,
		args: []Value{StringVal("a")},
	},
	{
		name: "gt string",
		s: shape.Filter{
			From: shape.AllNodes{},
			Filters: []shape.ValueFilter{
				shape.Comparison{Op: iterator.CompareGT, Val: quad.String("a")},
			},
		},
		qu:   `SELECT hash AS ` + tagNode + ` FROM nodes WHERE value_string > $1 AND iri IS NULL AND bnode IS NULL AND datatype IS NULL AND language IS NULL`,
		args: []Value{StringVal("a")},
	},
	{
		name: "gt typed string",
		s: shape.Filter{
			From: shape.AllNodes{},
			Filters: []shape.ValueFilter{
				shape.Comparison{Op: iterator.CompareGT, Val: quad.TypedString{Value: "a", Type: "A"}},
			},
		},
		qu:   `SELECT hash AS ` + tagNode + ` FROM nodes WHERE value_string > $1 AND datatype = $2`,
		args: []Value{StringVal("a"), StringVal("A")},
	},
	{
		name: "lookup int",
		s: shape.Filter{
			From: shape.AllNodes{},
			Filters: []shape.ValueFilter{
				shape.Comparison{Op: iterator.CompareGT, Val: quad.Int(42)},
			},
		},
		qu:   `SELECT hash AS ` + tagNode + ` FROM nodes WHERE value_int > $1`,
		args: []Value{IntVal(42)},
	},
	{
		name: "all quads",
		s:    shape.Quads{},
		qu: `SELECT t_1.subject_hash AS __subject, t_1.predicate_hash AS __predicate, t_1.object_hash AS __object, t_1.label_hash AS __label
	FROM quads AS t_1`,
	},
	{
		name: "limit quads and skip first",
		s:    shape.Page{From: shape.Quads{}, Limit: 100, Skip: 1},
		qu: `SELECT t_1.subject_hash AS __subject, t_1.predicate_hash AS __predicate, t_1.object_hash AS __object, t_1.label_hash AS __label
	FROM quads AS t_1
	LIMIT 100
	OFFSET 1`,
	},
	{
		name: "quads with subject and predicate",
		s: shape.Quads{
			{Dir: quad.Subject, Values: shape.Fixed{sVal("s")}},
			{Dir: quad.Predicate, Values: shape.Fixed{sVal("p")}},
		},
		qu: `SELECT t_1.subject_hash AS __subject, t_1.predicate_hash AS __predicate, t_1.object_hash AS __object, t_1.label_hash AS __label
	FROM quads AS t_1
	WHERE t_1.subject_hash = $1 AND t_1.predicate_hash = $2`,
		args: sVals("s", "p"),
	},
	{
		name: "quad actions",
		s: shape.QuadsAction{
			Result: quad.Subject,
			Save: map[quad.Direction][]string{
				quad.Object: {"o1", "o2"},
				quad.Label:  {"l 1"},
			},
			Filter: map[quad.Direction]graph.Ref{
				quad.Predicate: sVal("p"),
			},
		},
		qu: `SELECT subject_hash AS ` + tagNode + `, object_hash AS o1, object_hash AS o2, label_hash AS "l 1"
	FROM quads
	WHERE predicate_hash = $1`,
		args: sVals("p"),
	},
	{
		name: "quad actions and save",
		s: shape.Save{
			Tags: []string{"sub"},
			From: shape.QuadsAction{
				Result: quad.Subject,
				Save: map[quad.Direction][]string{
					quad.Object: {"o1", "o2"},
					quad.Label:  {"l 1"},
				},
				Filter: map[quad.Direction]graph.Ref{
					quad.Predicate: sVal("p"),
				},
			},
		},
		qu: `SELECT subject_hash AS sub, subject_hash AS ` + tagNode + `, object_hash AS o1, object_hash AS o2, label_hash AS "l 1"
	FROM quads
	WHERE predicate_hash = $1`,
		args: sVals("p"),
	},
	{
		name: "quads with subquery",
		s: shape.Quads{
			{Dir: quad.Subject, Values: shape.Fixed{sVal("s")}},
			{
				Dir: quad.Predicate,
				Values: shape.QuadsAction{
					Result: quad.Subject,
					Filter: map[quad.Direction]graph.Ref{
						quad.Predicate: sVal("p"),
					},
				},
			},
		},
		qu: `SELECT t_1.subject_hash AS __subject, t_1.predicate_hash AS __predicate, t_1.object_hash AS __object, t_1.label_hash AS __label
	FROM quads AS t_1, (SELECT subject_hash AS ` + tagNode + ` FROM quads WHERE predicate_hash = $1) AS t_2
	WHERE t_1.subject_hash = $2 AND t_1.predicate_hash = t_2.` + tagNode,
		args: sVals("p", "s"),
	},
	{
		name: "quads with subquery (inner tags)",
		s: shape.Quads{
			{Dir: quad.Subject, Values: shape.Fixed{sVal("s")}},
			{
				Dir: quad.Predicate,
				Values: shape.Save{
					Tags: []string{"pred"},
					From: shape.QuadsAction{
						Result: quad.Subject,
						Save: map[quad.Direction][]string{
							quad.Object: {"ob"},
						},
						Filter: map[quad.Direction]graph.Ref{
							quad.Predicate: sVal("p"),
						},
					},
				},
			},
		},
		qu: `SELECT t_1.subject_hash AS __subject, t_1.predicate_hash AS __predicate, t_1.object_hash AS __object, t_1.label_hash AS __label, t_2.subject_hash AS pred, t_2.object_hash AS ob
	FROM quads AS t_1, quads AS t_2
	WHERE t_1.subject_hash = $1 AND t_2.predicate_hash = $2 AND t_1.predicate_hash = t_2.subject_hash`,
		args: sVals("s", "p"),
	},
	{
		name: "quads with subquery (limit)",
		s: shape.Quads{
			{Dir: quad.Subject, Values: shape.Fixed{sVal("s")}},
			{
				Dir: quad.Predicate,
				Values: shape.Page{
					Limit: 10,
					From: shape.QuadsAction{
						Result: quad.Subject,
						Filter: map[quad.Direction]graph.Ref{
							quad.Predicate: sVal("p"),
						},
					},
				},
			},
		},
		qu: `SELECT t_1.subject_hash AS __subject, t_1.predicate_hash AS __predicate, t_1.object_hash AS __object, t_1.label_hash AS __label
	FROM quads AS t_1, (SELECT subject_hash AS ` + tagNode + ` FROM quads WHERE predicate_hash = $1 LIMIT 10) AS t_2
	WHERE t_1.subject_hash = $2 AND t_1.predicate_hash = t_2.` + tagNode,
		args: sVals("p", "s"),
	},
	{
		skip: true, // TODO
		name: "quads with subquery (inner tags + limit)",
		s: shape.Quads{
			{Dir: quad.Subject, Values: shape.Fixed{sVal("s")}},
			{
				Dir: quad.Predicate,
				Values: shape.Save{
					Tags: []string{"pred"},
					From: shape.Page{
						Limit: 10,
						From: shape.QuadsAction{
							Result: quad.Subject,
							Save: map[quad.Direction][]string{
								quad.Object: {"ob"},
							},
							Filter: map[quad.Direction]graph.Ref{
								quad.Predicate: sVal("p"),
							},
						},
					},
				},
			},
		},
		qu:   ``,
		args: []Value{},
	},
	{
		name: "nodes from quads",
		s: shape.NodesFrom{
			Dir: quad.Object,
			Quads: shape.Quads{
				{Dir: quad.Subject, Values: shape.Fixed{sVal("s")}},
				{
					Dir: quad.Predicate,
					Values: shape.QuadsAction{
						Result: quad.Subject,
						Save: map[quad.Direction][]string{
							quad.Object: {"ob"},
						},
						Filter: map[quad.Direction]graph.Ref{
							quad.Predicate: sVal("p"),
						},
					},
				},
			},
		},
		qu: `SELECT t_1.object_hash AS ` + tagNode + `, t_2.object_hash AS ob
	FROM quads AS t_1, quads AS t_2
	WHERE t_1.subject_hash = $1 AND t_2.predicate_hash = $2 AND t_1.predicate_hash = t_2.subject_hash`,
		args: sVals("s", "p"),
	},
	{
		name: "intersect selects",
		s: shape.Intersect{
			shape.Save{
				Tags: []string{"sub"},
				From: shape.QuadsAction{
					Result: quad.Subject,
					Save: map[quad.Direction][]string{
						quad.Object: {"o1"},
						quad.Label:  {"l 1"},
					},
					Filter: map[quad.Direction]graph.Ref{
						quad.Predicate: sVal("p1"),
					},
				},
			},
			shape.NodesFrom{
				Dir: quad.Object,
				Quads: shape.Quads{
					{Dir: quad.Subject, Values: shape.Fixed{sVal("s")}},
					{
						Dir: quad.Predicate,
						Values: shape.QuadsAction{
							Result: quad.Subject,
							Save: map[quad.Direction][]string{
								quad.Object: {"ob"},
							},
							Filter: map[quad.Direction]graph.Ref{
								quad.Predicate: sVal("p2"),
							},
						},
					},
				},
			},
		},
		qu: `SELECT t_3.subject_hash AS sub, t_3.subject_hash AS __node, t_3.object_hash AS o1, t_3.label_hash AS "l 1", t_2.object_hash AS ob
	FROM quads AS t_3, quads AS t_1, quads AS t_2
	WHERE t_3.predicate_hash = $1 AND t_1.subject_hash = $2 AND t_2.predicate_hash = $3 AND t_1.predicate_hash = t_2.subject_hash AND t_3.subject_hash = t_1.object_hash`,
		args: sVals("p1", "s", "p2"),
	},
	{
		name: "deep shape",
		s: shape.NodesFrom{
			Dir: quad.Object,
			Quads: shape.Quads{
				shape.QuadFilter{Dir: quad.Predicate, Values: shape.Fixed{sVal("s")}},
				shape.QuadFilter{
					Dir: quad.Subject,
					Values: shape.NodesFrom{
						Dir: quad.Subject,
						Quads: shape.Quads{
							shape.QuadFilter{Dir: quad.Predicate, Values: shape.Fixed{sVal("s")}},
							shape.QuadFilter{
								Dir: quad.Object,
								Values: shape.NodesFrom{
									Dir: quad.Subject,
									Quads: shape.Quads{
										shape.QuadFilter{Dir: quad.Predicate, Values: shape.Fixed{sVal("a")}},
										shape.QuadFilter{
											Dir: quad.Object,
											Values: shape.QuadsAction{
												Result: quad.Subject,
												Filter: map[quad.Direction]graph.Ref{
													quad.Predicate: sVal("n"),
													quad.Object:    sVal("k"),
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		qu:   `SELECT t_5.object_hash AS __node FROM quads AS t_5, (SELECT t_3.subject_hash AS __node FROM quads AS t_3, (SELECT t_1.subject_hash AS __node FROM quads AS t_1, (SELECT subject_hash AS __node FROM quads WHERE predicate_hash = $1 AND object_hash = $2) AS t_2 WHERE t_1.predicate_hash = $3 AND t_1.object_hash = t_2.__node) AS t_4 WHERE t_3.predicate_hash = $4 AND t_3.object_hash = t_4.__node) AS t_6 WHERE t_5.predicate_hash = $5 AND t_5.subject_hash = t_6.__node`,
		args: sVals("n", "k", "a", "s", "s"),
	},
}

func TestSQLShapes(t *testing.T) {
	dialect := DefaultDialect
	dialect.Placeholder = func(i int) string {
		return fmt.Sprintf("$%d", i)
	}
	for _, c := range shapeCases {
		t.Run(c.name, func(t *testing.T) {
			opt := NewOptimizer()
			s, ok := c.s.Optimize(opt)
			if c.skip {
				t.Skipf("%#v", s)
			}
			require.True(t, ok, "%#v", s)
			sq, ok := s.(Shape)
			require.True(t, ok, "%#v", s)
			b := NewBuilder(dialect)
			require.Equal(t, c.qu, sq.SQL(b), "%#v", sq)
			require.Equal(t, c.args, sq.Args())
		})
	}
}
