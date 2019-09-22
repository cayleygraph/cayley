package main

import (
	"fmt"
	"log"

	"github.com/cayleygraph/cayley"
	"github.com/cayleygraph/quad"
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
	// 2. Quad store, but we can omit it because we have already built path with it.
	err = p.Iterate(nil).EachValue(nil, func(value quad.Value) {
		nativeValue := quad.NativeOf(value) // this converts RDF values to normal Go types
		fmt.Println(nativeValue)
	})
	if err != nil {
		log.Fatalln(err)
	}
}
