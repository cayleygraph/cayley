package ouch

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/nosql"
	"github.com/cayleygraph/cayley/graph/nosql/nosqltest"
	"github.com/go-kivik/kivik"
)

const remote = "http://testusername:testpassword@127.0.0.1:5984/ouchtest"

var dbId int // PouchDB requires a different DB name each time, or it uses cached data!

func makeOuch(t testing.TB) (nosql.Database, graph.Options, func()) {
	var testDBname, homeDir, dir string
	var err error

	switch defaultDriverName {
	case "pouch":
		// TODO add remote db access tests
		testDBname = fmt.Sprintf("pouchdb%d.test", dbId) // see dbId comment
		dbId++

	case "couch":
		testDBname = remote

	default:
		panic("bad defaultDriverName: " + defaultDriverName)

	}

	ctx := context.TODO()
	if runtime.GOARCH == "js" && testDBname != remote {

		homeDir, err = os.Getwd()
		if err != nil {
			t.Fatal(err)
		}

		dir, err = ioutil.TempDir("", "pouch-")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Chdir(dir); err != nil { // in js, run the tests from the temp dir
			t.Fatal(err)
		}

		// just check the dir is empty
		fi, err := ioutil.ReadDir(".")
		if err != nil {
			t.Fatal(err)
		}
		for _, thisFile := range fi {
			fn := thisFile.Name()
			if strings.HasPrefix(fn, "pouchdb") && strings.Contains(fn, ".test") {
				dbDir := thisFile.Name()
				if err := os.RemoveAll(dbDir); err != nil {
					t.Fatal(err)
				}
			}
		}
	} else {
		client, err := kivik.New(ctx, defaultDriverName, testDBname)
		if err != nil {
			t.Fatal(err)
		}
		err = client.DestroyDB(ctx, testDBname)
		if err != nil {
			t.Log(err)
		}
	}

	qs, err := dialDB(true, testDBname, nil)
	if err != nil {
		t.Fatal(err)
	}

	return qs, nil, func() {
		qs.Close()
		if runtime.GOARCH == "js" && testDBname != remote {
			if err := os.RemoveAll(dir); err != nil { // remove the test data
				t.Fatal(err)
			}
			if err := os.Chdir(homeDir); err != nil { // go back where we came from
				t.Fatal(err)
			}
		}
	}
}

func TestIntStr(t *testing.T) {
	testI := []int64{120000, -4, 88, 0, -7000000, 88, math.MaxInt64, math.MinInt64}
	testS := []string{}
	for _, v := range testI {
		testS = append(testS, itos(v))
	}
	sort.Strings(testS)
	sort.Slice(testI, func(i, j int) bool { return testI[i] < testI[j] })
	for k, v := range testS {
		r := stoi(v)
		if r != testI[k] {
			t.Errorf("Sorting of stringed int64s wrong: %v %v %v", k, r, testI[k])
		}
	}
}

func TestOuchAll(t *testing.T) {
	nosqltest.TestAll(t, makeOuch, &nosqltest.Config{
		TimeInMs:   false,
		FloatToInt: false,
	})
}
