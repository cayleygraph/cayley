# Quickstart as Library

Currently, Cayley supports being used as a Go library for other projects. To use it in such a way, here's a quick example:

```go
package main

import (
  "log"

  "github.com/cayleygraph/cayley"
)

func main() {
  store, err := cayley.NewMemoryGraph()
  if err != nil {
    log.Fatalln(err)
  }
  store.AddQuad(cayley.Quad("food", "is", "good", ""))
  p := cayley.StartPath(store, "food").Out("is")
  it := p.BuildIterator()
  for cayley.RawNext(it) {
    log.Println(store.NameOf(it.Result()))
  }
}
```

To use other backends, you can empty-import them, eg

```go
import _ "github.com/cayleygraph/cayley/graph/bolt"
```

And use them with a call like

```go
import "github.com/cayleygraph/cayley/graph"

func open() {
  // Initialize the database
  graph.InitQuadStore("bolt", path, nil)

  // Open and use the database
  cayley.NewGraph("bolt", path, nil)
}
```

---

Another example of using it as a library from a users gist: 

https://gist.github.com/ejemba/6934c4784ef55fbd3aee

```go
package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/cayleygraph/cayley"
	"github.com/cayleygraph/cayley/graph"
	_ "github.com/cayleygraph/cayley/graph/bolt"
	"github.com/cayleygraph/cayley/internal/config"
	"github.com/cayleygraph/cayley/internal/db"
)

var store *cayley.Handle

// Open opens a Cayley database, creating it if necessary and return its handle
func Open(dbType, dbPath string) error {
	if store != nil {
		return errors.New("Could not open database : a database is already opened.")
	}

	var err error

	// Does it exist ?
	if dbType == "bolt" || dbType == "leveldb" {
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			// No, initialize it if possible

			err = db.Init(&config.Config{DatabasePath: dbPath, DatabaseType: dbType})
			if err != nil {
				return err
			}
		}
	}
	if dbType == "sql" {
		db.Init(&config.Config{DatabasePath: dbPath, DatabaseType: dbType})
	}

	store, err = cayley.NewGraph(dbType, dbPath, nil)
	if err != nil {
		return errors.New("Could not open database")
	}

	return nil
}

// Close closes a Cayley database
func Close() {
	if store != nil {
		store.Close()
		store = nil
	}
}

func main() {
	err := Open("bolt", "/tmp/boltdb")
	if err != nil {
		panic(err.Error())
	}
	defer Close()

	// Insert quads in a transaction
	t := cayley.NewTransaction()
	t.AddQuad(cayley.Quad("cayley", "is", "awesome", ""))
	t.AddQuad(cayley.Quad("cayley", "is", "simple", ""))
	store.ApplyTransaction(t)

	// Select quads
	it, _ := cayley.StartPath(store, "cayley").Out("is").BuildIterator().Optimize() // Do not forget to use Optimize() otherwise it will be really slow.
	defer it.Close()                                                                // Do not forget to close the iterator to avoid exhausting connections
	for cayley.RawNext(it) {
		if it.Result() != nil {
			fmt.Println(store.NameOf(it.Result()))
		}
	}
	if it.Err() != nil {
		fmt.Println(it.Err().Error()) // May happen if you loose the database connection for example
	}

	// Select quads using tags
	it, _ = cayley.StartPath(store, "cayley").Save("is", "is").BuildIterator().Optimize()
	defer it.Close()
	for cayley.RawNext(it) {
		if it.Result() != nil {
			tags := make(map[string]graph.Value)
			it.TagResults(tags)

			fmt.Println(store.NameOf(tags["is"]))
		}
	}
	if it.Err() != nil {
		fmt.Println(it.Err().Error())
	}
}
```
