package all

import (
	// import all implementations that hidalgo supports
	_ "github.com/hidal-go/hidalgo/kv/all"

	// make sure to import kv package, so it can re-register hidalgo's backends
	_ "github.com/cayleygraph/cayley/graph/kv"
	// legacy: override bolt implementation; check the package for details
	_ "github.com/cayleygraph/cayley/graph/kv/bolt"
)
