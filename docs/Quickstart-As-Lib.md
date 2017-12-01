# Quickstart as Library

Currently, Cayley supports being used as a Go library for other projects. To use it in such a way, here's a quick example:

```go
package main

import (
	"fmt"
	"log"

	"github.com/cayleygraph/cayley"
	"github.com/cayleygraph/cayley/quad"
)

func main() {
	// Create a brand new graph
	store, err := cayley.NewMemoryGraph()
	if err != nil {
		log.Fatalln(err)
	}

	store.AddQuad(quad.Make("phrase of the day", "is of course", "Hello World!", nil))

	// Now we create the path, to get to our data
	p := cayley.StartPath(store, quad.String("phrase of the day")).Out(quad.String("is of course"))

	// Now we iterate over results. Arguments:
	// 1. Optional context used for cancellation.
	// 2. Flag to optimize query before execution.
	// 3. Quad store, but we can omit it because we have already built path with it.
	err = p.Iterate(nil).EachValue(nil, func(value quad.Value){
		nativeValue := quad.NativeOf(value) // this converts RDF values to normal Go types
		fmt.Println(nativeValue)
	})
	if err != nil {
		log.Fatalln(err)
	}
}
```

To use other backends, you can empty-import them, eg

```go
import _ "github.com/cayleygraph/cayley/graph/kv/bolt"
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

More runnable examples are available in [examples](../examples/) folder.