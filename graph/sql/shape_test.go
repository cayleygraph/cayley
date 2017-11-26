package sql

import (
	"fmt"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/shape"
	"github.com/cayleygraph/cayley/quad"
	"github.com/stretchr/testify/require"
)

func hashVal(s string) NodeHash {
	return HashOf(quad.IRI(s))
}

func hashVals(arr ...string) []Value {
	out := make([]Value, 0, len(arr))
	for _, s := range arr {
		out = append(out, hashVal(s))
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
		name: "all quads",
		s:    shape.Quads{},
		qu:   `SELECT t_1.subject_hash AS __subject, t_1.predicate_hash AS __predicate, t_1.object_hash AS __object, t_1.label_hash AS __label FROM quads AS t_1`,
	},
	{
		name: "limit quads and skip first",
		s:    shape.Page{From: shape.Quads{}, Limit: 100, Skip: 1},
		qu:   `SELECT t_1.subject_hash AS __subject, t_1.predicate_hash AS __predicate, t_1.object_hash AS __object, t_1.label_hash AS __label FROM quads AS t_1 LIMIT 100 OFFSET 1`,
	},
	{
		name: "quads with subject and predicate",
		s: shape.Quads{
			{Dir: quad.Subject, Values: shape.Fixed{hashVal("s")}},
			{Dir: quad.Predicate, Values: shape.Fixed{hashVal("p")}},
		},
		qu:   `SELECT t_1.subject_hash AS __subject, t_1.predicate_hash AS __predicate, t_1.object_hash AS __object, t_1.label_hash AS __label FROM quads AS t_1 WHERE t_1.subject_hash = $1 AND t_1.predicate_hash = $2`,
		args: hashVals("s", "p"),
	},
	{
		name: "quad actions",
		s: shape.QuadsAction{
			Result: quad.Subject,
			Save: map[quad.Direction][]string{
				quad.Object: {"o1", "o2"},
				quad.Label:  {"l 1"},
			},
			Filter: map[quad.Direction]graph.Value{
				quad.Predicate: hashVal("p"),
			},
		},
		qu:   `SELECT subject_hash AS ` + tagNode + `, object_hash AS o1, object_hash AS o2, label_hash AS "l 1" FROM quads WHERE predicate_hash = $1`,
		args: hashVals("p"),
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
				Filter: map[quad.Direction]graph.Value{
					quad.Predicate: hashVal("p"),
				},
			},
		},
		qu:   `SELECT subject_hash AS sub, subject_hash AS ` + tagNode + `, object_hash AS o1, object_hash AS o2, label_hash AS "l 1" FROM quads WHERE predicate_hash = $1`,
		args: hashVals("p"),
	},
	{
		name: "quads with subquery",
		s: shape.Quads{
			{Dir: quad.Subject, Values: shape.Fixed{hashVal("s")}},
			{
				Dir: quad.Predicate,
				Values: shape.QuadsAction{
					Result: quad.Subject,
					Filter: map[quad.Direction]graph.Value{
						quad.Predicate: hashVal("p"),
					},
				},
			},
		},
		qu:   `SELECT t_1.subject_hash AS __subject, t_1.predicate_hash AS __predicate, t_1.object_hash AS __object, t_1.label_hash AS __label FROM quads AS t_1, (SELECT subject_hash AS ` + tagNode + ` FROM quads WHERE predicate_hash = $1) AS t_2 WHERE t_1.subject_hash = $2 AND t_1.predicate_hash = t_2.` + tagNode,
		args: hashVals("p", "s"),
	},
	{
		name: "quads with subquery (inner tags)",
		s: shape.Quads{
			{Dir: quad.Subject, Values: shape.Fixed{hashVal("s")}},
			{
				Dir: quad.Predicate,
				Values: shape.Save{
					Tags: []string{"pred"},
					From: shape.QuadsAction{
						Result: quad.Subject,
						Save: map[quad.Direction][]string{
							quad.Object: {"ob"},
						},
						Filter: map[quad.Direction]graph.Value{
							quad.Predicate: hashVal("p"),
						},
					},
				},
			},
		},
		qu:   `SELECT t_1.subject_hash AS __subject, t_1.predicate_hash AS __predicate, t_1.object_hash AS __object, t_1.label_hash AS __label, t_2.subject_hash AS pred, t_2.object_hash AS ob FROM quads AS t_1, quads AS t_2 WHERE t_1.subject_hash = $1 AND t_2.predicate_hash = $2 AND t_1.predicate_hash = t_2.subject_hash`,
		args: hashVals("s", "p"),
	},
	{
		name: "quads with subquery (limit)",
		s: shape.Quads{
			{Dir: quad.Subject, Values: shape.Fixed{hashVal("s")}},
			{
				Dir: quad.Predicate,
				Values: shape.Page{
					Limit: 10,
					From: shape.QuadsAction{
						Result: quad.Subject,
						Filter: map[quad.Direction]graph.Value{
							quad.Predicate: hashVal("p"),
						},
					},
				},
			},
		},
		qu:   `SELECT t_1.subject_hash AS __subject, t_1.predicate_hash AS __predicate, t_1.object_hash AS __object, t_1.label_hash AS __label FROM quads AS t_1, (SELECT subject_hash AS ` + tagNode + ` FROM quads WHERE predicate_hash = $1 LIMIT 10) AS t_2 WHERE t_1.subject_hash = $2 AND t_1.predicate_hash = t_2.` + tagNode,
		args: hashVals("p", "s"),
	},
	{
		skip: true, // TODO
		name: "quads with subquery (inner tags + limit)",
		s: shape.Quads{
			{Dir: quad.Subject, Values: shape.Fixed{hashVal("s")}},
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
							Filter: map[quad.Direction]graph.Value{
								quad.Predicate: hashVal("p"),
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
				{Dir: quad.Subject, Values: shape.Fixed{hashVal("s")}},
				{
					Dir: quad.Predicate,
					Values: shape.QuadsAction{
						Result: quad.Subject,
						Save: map[quad.Direction][]string{
							quad.Object: {"ob"},
						},
						Filter: map[quad.Direction]graph.Value{
							quad.Predicate: hashVal("p"),
						},
					},
				},
			},
		},
		qu:   `SELECT t_1.object_hash AS ` + tagNode + `, t_2.object_hash AS ob FROM quads AS t_1, quads AS t_2 WHERE t_1.subject_hash = $1 AND t_2.predicate_hash = $2 AND t_1.predicate_hash = t_2.subject_hash`,
		args: hashVals("s", "p"),
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
