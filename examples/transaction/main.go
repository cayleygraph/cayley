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
	t.AddQuad(quad.Quad{quad.String("food"), quad.String("is"), quad.String("good"), quad.String("demo graph")})
	t.AddQuad(quad.Quad{quad.String("phrase of the day"), quad.String("is of course"), quad.String("Hello World!"), quad.String("demo graph")})
	t.AddQuad(quad.Quad{quad.String("cats"), quad.String("are"), quad.String("awesome"), quad.String("demo graph")})
	t.AddQuad(quad.Quad{quad.String("cats"), quad.String("are"), quad.String("scary"), quad.String("demo graph")})
	t.AddQuad(quad.Quad{quad.String("cats"), quad.String("want to"), quad.String("kill you"), quad.String("demo graph")})

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
}
