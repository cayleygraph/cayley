// +build docker

package ouch

import (
	"context"
	"fmt"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/nosql"
	"github.com/cayleygraph/cayley/internal/dock"
	"github.com/go-kivik/kivik"
)

func makeCouchDB(t testing.TB) (nosql.Database, *nosql.Options, graph.Options, func()) {
	var conf dock.Config

	conf.Image = "couchdb:2"
	conf.Env = []string{
		"COUCHDB_USER=cayley",
		"COUCHDB_PASSWORD=cayley",
	}
	const port = "5984"
	addr, closer := dock.RunAndWait(t, conf, port, nil)
	qs, err := dialDB(true, "http://cayley:cayley@"+addr+"/cayley", nil)
	if err != nil {
		closer()
		t.Fatal(err)
	}
	return qs, &nosqlOptions, nil, func() {
		qs.Close()
		closer()
	}
}

var dbId int // PouchDB requires a different DB name each time, or it uses cached data!

func makePouchDB(t testing.TB) (nosql.Database, *nosql.Options, graph.Options, func()) {
	ctx := context.TODO()
	// TODO add remote db access tests
	name := fmt.Sprintf("pouchdb%d.test", dbId) // see dbId comment
	dbId++

	client, err := kivik.New(ctx, driverName, name)
	if err != nil {
		t.Fatal(err)
	}
	err = client.DestroyDB(ctx, name)
	if err != nil {
		t.Log(err)
	}

	qs, err := dialDB(true, name, nil)
	if err != nil {
		t.Fatal(err)
	}

	return qs, &nosqlOptions, nil, func() {
		qs.Close()
	}
}
