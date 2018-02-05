package nosql

import (
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

var filterMatch = []struct {
	f   FieldFilter
	d   Document
	exp bool
}{
	{
		f:   FieldFilter{Path: []string{"value", "str"}, Filter: GT, Value: String("f")},
		d:   Document{"value": Document{"str": String("bob")}},
		exp: false,
	},
	{
		f:   FieldFilter{Path: []string{"value", "str"}, Filter: Equal, Value: String("f")},
		d:   Document{"value": Document{"str": String("bob")}},
		exp: false,
	},
	{
		f:   FieldFilter{Path: []string{"value", "str"}, Filter: Equal, Value: String("bob")},
		d:   Document{"value": Document{"str": String("bob")}},
		exp: true,
	},
	{
		f:   FieldFilter{Path: []string{"value", "str"}, Filter: NotEqual, Value: String("bob")},
		d:   Document{"value1": Document{"str": String("bob")}},
		exp: true,
	},
}

func TestFilterMatch(t *testing.T) {
	for _, c := range filterMatch {
		require.Equal(t, c.exp, c.f.Matches(c.d))
	}
}

func TestIntStr(t *testing.T) {
	var testS []string
	testI := []int64{
		120000, -4, 88, 0, -7000000, 88,
		math.MaxInt64 - 1, math.MaxInt64,
		math.MinInt64, math.MinInt64 + 1,
	}
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
