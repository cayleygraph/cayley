// +build js

package ouch

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/nosql"
	"github.com/go-kivik/kivik"
)

func makeCouchDB(t testing.TB) (nosql.Database, *nosql.Options, graph.Options, func()) {
	t.SkipNow()
	return nil, nil, nil, nil
}

func makePouchDB(t testing.TB) (nosql.Database, *nosql.Options, graph.Options, func()) {
	if runtime.GOARCH != "js" {
		panic("not js")
	}

	dir, err := ioutil.TempDir("", "pouch-")
	if err != nil {
		t.Fatal("failed to make temp dir:", err)
	}

	name := fmt.Sprintf("cayley-%d", rand.Int())

	qs, err := dialDB(false, dir+"/"+name, nil)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}

	return qs, &nosqlOptions, nil, func() {
		qs.Close()
		ctx := context.TODO()
		if c, err := kivik.New(ctx, driverName, dir); err == nil {
			_ = c.DestroyDB(ctx, name)
		}
		if err := os.RemoveAll(dir); err != nil { // remove the test data
			t.Fatal(err)
		}
	}
}
