package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/cayleygraph/cayley"
	"github.com/cayleygraph/cayley/graph"
	_ "github.com/cayleygraph/cayley/graph/bolt"
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

type quadWriter struct {
	w graph.QuadWriter
}

func (w quadWriter) WriteQuad(q quad.Quad) error {
	return w.w.AddQuad(q)
}

func main() {
	// Define an "ex:" prefix for IRIs that will be expanded to "http://example.org".
	// "ex:name" will become "http://example.org/name"
	voc.RegisterPrefix("ex:", "http://example.org/")

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
	defer store.Close()
	qw := quadWriter{store.QuadWriter} // TODO: temporary workaround before formats PR merge

	// Save an object
	bob := Person{
		ID:   quad.IRI("ex:bob"),
		Name: "Bob", Age: 32,
	}
	fmt.Printf("saving: %+v\n", bob)
	id, err := schema.WriteAsQuads(qw, bob)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("id for object:", id, "=", bob.ID) // should be equal

	// Get object by id
	var someone Person
	err = schema.SaveTo(nil, store, &someone, id)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("loaded: %+v\n", someone)

	// Or get all objects of type Person
	var people []Person
	err = schema.SaveTo(nil, store, &people)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("loaded slice: %+v\n", people)
}
