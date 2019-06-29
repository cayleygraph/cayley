//+build cgo

package all

import (
	// backends requiring cgo
	_ "github.com/cayleygraph/cayley/graph/sql/sqlite"
)
