package main

import (
	"fmt"
	"log"

	"github.com/cayleygraph/cayley"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
)

func main() {
	// To see how most of this works, see hello_world -- this just add in a transaction
	store, err := cayley.NewMemoryGraph()
	if err != nil {
		log.Fatalln(err)
	}

	// Create a transaction of work to do
	// NOTE: the transaction is independant of the storage type, so comes from cayley rather than store
	t := cayley.NewTransaction()
	t.AddQuad(quad.Make("food", "is", "good", nil))
	t.AddQuad(quad.Make("phrase of the day", "is of course", "Hello World!", nil))
	t.AddQuad(quad.Make("cats", "are", "awesome", nil))
	t.AddQuad(quad.Make("cats", "are", "scary", nil))
	t.AddQuad(quad.Make("cats", "want to", "kill you", nil))

	// Apply the transaction
	err = store.ApplyTransaction(t)
	if err != nil {
		log.Fatalln(err)
	}

	p := cayley.StartPath(store, quad.String("cats")).Out(quad.String("are"))
	it, _ := p.BuildIterator().Optimize()
	defer it.Close()

	nxt := graph.AsNexter(it)
	defer nxt.Close()
	for nxt.Next() {
		fmt.Println("cats are", store.NameOf(it.Result()).Native())
	}
	if err = nxt.Err(); err != nil {
		log.Fatalln(err)
	}
}
