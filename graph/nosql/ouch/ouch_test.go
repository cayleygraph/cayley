package ouch

import (
	"math"
	"sort"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/nosql"
	"github.com/cayleygraph/cayley/graph/nosql/nosqltest"
)

func makeOuch(t testing.TB) (nosql.Database, graph.Options, func()) {
	switch driverName {
	default:
		t.Fatal("unknown driverName: ", driverName)
		return nil, nil, nil
	case "couch":
		return makeCouchDB(t)
	case "pouch":
		return makePouchDB(t)
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
