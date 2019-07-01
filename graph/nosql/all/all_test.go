// +build docker

package all

import (
	"testing"

	_ "github.com/hidal-go/hidalgo/legacy/nosql/nosqltest/all"

	"github.com/cayleygraph/cayley/graph/nosql/nosqltest"
	hnosqltest "github.com/hidal-go/hidalgo/legacy/nosql/nosqltest"
)

func TestNoSQL(t *testing.T) {
	hnosqltest.RunTest(t, nosqltest.TestAll)
}

func BenchmarkNoSQL(t *testing.B) {
	hnosqltest.RunBenchmark(t, nosqltest.BenchmarkAll)
}
