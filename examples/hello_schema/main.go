package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"

	"github.com/cayleygraph/cayley"
	"github.com/cayleygraph/cayley/graph"
	_ "github.com/cayleygraph/cayley/graph/kv/bolt"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/schema"
	"github.com/cayleygraph/cayley/voc"
	// Import RDF vocabulary definitions to be able to expand IRIs like rdf:label.
	_ "github.com/cayleygraph/cayley/voc/core"
)

type Person struct {
	// dummy field to enforce all object to have a <id> <rdf:type> <ex:Person> relation
	// means nothing for Go itself
	rdfType struct{} `quad:"@type > ex:Person"`
	ID      quad.IRI `json:"@id"`     // tag @id is a special one - graph node value will be stored in this field
	Name    string   `json:"ex:name"` // field name (predicate) may be written as json field name
	Age     int      `quad:"ex:age"`  // or in a quad tag
}

type Coords struct {
	// Object may be without id - it will be generated automatically.
	// It's also not necessary to have a type definition.
	Lat float64 `json:"ex:lat"`
	Lng float64 `json:"ex:lng"`
}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	// Define an "ex:" prefix for IRIs that will be expanded to "http://example.org".
	// "ex:name" will become "http://example.org/name"
	voc.RegisterPrefix("ex:", "http://example.org/")

	// Associate Go type with an IRI.
	// All Coords objects will now generate a <id> <rdf:type> <ex:Coords> triple.
	schema.RegisterType(quad.IRI("ex:Coords"), Coords{})

	// Override a function to generate IDs. Can be changed to generate UUIDs, for example.
	schema.GenerateID = func(_ interface{}) quad.Value {
		return quad.BNode(fmt.Sprintf("node%d", rand.Intn(1000)))
	}

	// File for your new BoltDB. Use path to regular file and not temporary in the real world
	tmpfile, err := ioutil.TempFile("", "example")
	checkErr(err)

	defer os.Remove(tmpfile.Name()) // clean up

	// Initialize the database
	graph.InitQuadStore("bolt", tmpfile.Name(), nil)

	// Open and use the database
	store, err := cayley.NewGraph("bolt", tmpfile.Name(), nil)
	checkErr(err)
	defer store.Close()
	qw := graph.NewWriter(store)

	// Save an object
	bob := Person{
		ID:   quad.IRI("ex:bob").Full().Short(),
		Name: "Bob", Age: 32,
	}
	fmt.Printf("saving: %+v\n", bob)
	id, err := schema.WriteAsQuads(qw, bob)
	checkErr(err)
	err = qw.Close()
	checkErr(err)

	fmt.Println("id for object:", id, "=", bob.ID) // should be equal

	// Get object by id
	var someone Person
	err = schema.LoadTo(nil, store, &someone, id)
	checkErr(err)
	fmt.Printf("loaded: %+v\n", someone)

	// Or get all objects of type Person
	var people []Person
	err = schema.LoadTo(nil, store, &people)
	checkErr(err)
	fmt.Printf("people: %+v\n", people)

	fmt.Println()

	// Store objects with no ID and type
	coords := []Coords{
		{Lat: 12.3, Lng: 34.5},
		{Lat: 39.7, Lng: 8.41},
	}
	qw = graph.NewWriter(store)
	for _, c := range coords {
		id, err = schema.WriteAsQuads(qw, c)
		checkErr(err)
		fmt.Println("generated id:", id)
	}
	err = qw.Close()
	checkErr(err)

	// Get coords back
	var newCoords []Coords
	err = schema.LoadTo(nil, store, &newCoords)
	checkErr(err)
	fmt.Printf("coords: %+v\n", newCoords)

	// Print quads
	fmt.Println("\nquads:")
	it := store.QuadsAllIterator()
	for it.Next() {
		fmt.Println(store.Quad(it.Result()))
	}
}
