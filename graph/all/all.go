package all

import (
	// legacy backends (will be deprecated)
	_ "github.com/cayleygraph/cayley/graph/bolt"
	_ "github.com/cayleygraph/cayley/graph/leveldb"
	_ "github.com/cayleygraph/cayley/graph/mongo"

	// supported backends
	_ "github.com/cayleygraph/cayley/graph/kv/bolt"
	_ "github.com/cayleygraph/cayley/graph/kv/btree"
	_ "github.com/cayleygraph/cayley/graph/kv/leveldb"
	_ "github.com/cayleygraph/cayley/graph/memstore"
	_ "github.com/cayleygraph/cayley/graph/nosql/elastic"
	_ "github.com/cayleygraph/cayley/graph/nosql/mongo"
	_ "github.com/cayleygraph/cayley/graph/sql/cockroach"
	_ "github.com/cayleygraph/cayley/graph/sql/mysql"
	_ "github.com/cayleygraph/cayley/graph/sql/postgres"
)
