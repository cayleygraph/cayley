package nosql

import (
	"math"
	"sort"
	"testing"
)

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
