package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/cayleygraph/cayley"
	"github.com/cayleygraph/cayley/graph"
	_ "github.com/cayleygraph/cayley/graph/kv/bolt"
	"github.com/cayleygraph/cayley/quad"
)

func main() {
	// File for your new BoltDB. Use path to regular file and not temporary in the real world
	tmpfile, err := ioutil.TempFile("", "example")
	if err != nil {
		log.Fatal(err)
	}

	defer os.Remove(tmpfile.Name()) // clean up

	// Initialize the database
	graph.InitQuadStore("bolt", tmpfile.Name(), nil)

	// Open and use the database
	store, err := cayley.NewGraph("bolt", tmpfile.Name(), nil)
	if err != nil {
		log.Fatalln(err)
	}

	store.AddQuad(quad.Make("phrase of the day", "is of course", "Hello BoltDB!", "demo graph"))

	// Now we create the path, to get to our data
	p := cayley.StartPath(store, quad.String("phrase of the day")).Out(quad.String("is of course"))

	// This is more advanced example of the query.
	// Simpler equivalent can be found in hello_world example.

	// Now we get an iterator for the path and optimize it.
	// The second return is if it was optimized, but we don't care for now.
	it, _ := p.BuildIterator().Optimize()

	// Optimize iterator on quad store level.
	// After this step iterators will be replaced with backend-specific ones.
	it, _ = store.OptimizeIterator(it)

	// remember to cleanup after yourself
	defer it.Close()

	// While we have items
	for it.Next() {
		token := it.Result()                // get a ref to a node (backend-specific)
		value := store.NameOf(token)        // get the value in the node (RDF)
		nativeValue := quad.NativeOf(value) // convert value to normal Go type

		fmt.Println(nativeValue) // print it!
	}
	if err := it.Err(); err != nil {
		log.Fatalln(err)
	}
}
