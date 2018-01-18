package ouch

import (
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/nosql"
	"github.com/cayleygraph/cayley/graph/nosql/nosqltest"
	"github.com/stretchr/testify/require"
)

func makeOuch(t testing.TB) (nosql.Database, *nosql.Options, graph.Options, func()) {
	switch driverName {
	default:
		t.Fatal("unknown driverName: ", driverName)
		return nil, nil, nil, nil
	case "couch":
		return makeCouchDB(t)
	case "pouch":
		return makePouchDB(t)
	}
}

func TestOuchAll(t *testing.T) {
	nosqltest.TestAll(t, makeOuch, &nosqltest.Config{
		IntToFloat: true,
		PageSize:   25,
	})
}

func TestSelector(t *testing.T) {
	q := ouchQuery{"selector": make(map[string]interface{})}
	in := []interface{}{"a", "b"}
	q.putSelector(idField, map[string]interface{}{"$in": in})
	q.putSelector(idField, map[string]interface{}{"$gt": "a"})
	require.Equal(t, ouchQuery{
		"selector": map[string]interface{}{
			idField: map[string]interface{}{
				"$in": in,
				"$gt": "a",
			},
		},
	}, q)
}
