package main

import (
	"fmt"
	"log"

	"github.com/cayleygraph/cayley"
	"github.com/cayleygraph/cayley/graph"
	_ "github.com/cayleygraph/cayley/graph/bolt"
	"github.com/cayleygraph/cayley/quad"
)

func main() {
	// path to your new BoltDB
	path := "./db"

	// Initialize the database
	graph.InitQuadStore("bolt", path, nil)

	// Open and use the database
	store, err := cayley.NewGraph("bolt", path, nil)
	if err != nil {
		log.Fatalln(err)
	}

	store.AddQuad(quad.Make("phrase of the day", "is of course", "Hello BoltDB!", "demo graph"))

	// Now we create the path, to get to our data
	p := cayley.StartPath(store, quad.String("phrase of the day")).Out(quad.String("is of course"))

	// Now we get an iterator for the path (and optimize it, the second return is if it was optimized,
	// but we don't care for now)
	it, _ := p.BuildIterator().Optimize()

	// Now for each time we can go to next iterator
	nxt := graph.AsNexter(it)
	// remember to cleanup after yourself. Closing the nexter closes the iterator as well
	defer nxt.Close()

	// While we have items
	for nxt.Next() {
		token := it.Result()                // get a ref to a node
		value := store.NameOf(token)        // get the value in the node
		nativeValue := quad.NativeOf(value) // this converts nquad values to normal Go type

		fmt.Println(nativeValue) // print it!
	}
	if err := nxt.Err(); err != nil {
		log.Fatalln(err)
	}
}
