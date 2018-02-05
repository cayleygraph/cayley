// +build !docker,!js

package ouch

import (
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/nosql"
)

func selectImpl(t testing.TB) {
	t.Skipf("enable docker build tag or test via gopherjs")
}

func makeCouchDB(t testing.TB) (nosql.Database, *nosql.Options, graph.Options, func()) {
	selectImpl(t)
	return nil, nil, nil, nil
}

func makePouchDB(t testing.TB) (nosql.Database, *nosql.Options, graph.Options, func()) {
	selectImpl(t)
	return nil, nil, nil, nil
}
