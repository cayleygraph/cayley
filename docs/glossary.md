# Glossary of Terms
*Note: this definitions in this glossary are sequenced so that they build on each other, one to the next, rather than alphabetically.*

### triple
1. a data entity composed of subject-predicate-object, like "Bob is 35" or "Bob knows Fred". (A predicate in traditional grammar...is seen as a property that a subject has or is characterized by.) [source](https://en.wikipedia.org/wiki/Triplestore) and [source](https://en.wikipedia.org/wiki/Predicate_(grammar)#Predicates_in_traditional_grammar)

### triplestore
1. a purpose-built database for the storage and retrieval of triples... [source](https://en.wikipedia.org/wiki/Triplestore)

### quad
1. where triples have the form `{subject, predicate, object}`, quads would have a form along the lines of `{subject, predicate, object, context}` [source](https://en.wikipedia.org/wiki/Named_graph#Named_graphs_and_quads) 
2. You can add context or extra values to triples that identifies them and makes it easy to define subgraphs, or named properties. [source](https://neo4j.com/blog/rdf-triple-store-vs-labeled-property-graph-difference/)
3. From [Cayley godoc](https://godoc.org/github.com/cayleygraph/cayley/quad#Quad):
```go
type Quad struct {
    Subject   Value `json:“subject”`
    Predicate Value `json:“predicate”`
    Object    Value `json:“object”`
    Label     Value `json:“label,omitempty”`
}
```

### link
1. Another name for a triple, since it "links" any two nodes.
2. Given the triple `{A, knows, C}` you would say in graph terminology that `A` and `C` are "vertices" while `knows` is an "edge". You would also say that `A`, `knows`, and `C` are all "nodes", and they are "linked" to one another by the triple.

### IRI
1. IRI is an RDF Internationalized Resource Identifier. [source](https://godoc.org/github.com/cayleygraph/cayley/quad#IRI)
2. An IRI (Internationalized Resource Identifier) within an RDF graph is a Unicode string that conforms to the syntax defined in RFC 3987. [source](https://www.w3.org/TR/rdf11-concepts/#h3_section-IRIs)
3. IRIs are a generalization of URIs that permits a wider range of Unicode characters. Every absolute URI and URL is an IRI, but not every IRI is an URI. [source](https://www.w3.org/TR/rdf11-concepts/#h3_section-IRIs)

### RDF
1. [Resource Description Framework](https://en.wikipedia.org/wiki/Resource_Description_Framework), basically a set of standards defined around quads
2. An RDF triple consists of three components: 
	1. the subject, which is an IRI or a blank node
	2. the predicate, which is an IRI
	3. the object, which is an IRI, a literal or a blank node [source](https://www.w3.org/TR/rdf11-concepts/#h3_section-triples)   

### RDF store, quad store, named graph, semantic graph database
1. ...persisting RDF — storing it — became a thing, and these stores were called triple stores. Next they were called quad stores and included information about context and named graphs, then RDF stores, and most recently they call themselves “semantic graph database.” [source](https://neo4j.com/blog/rdf-triple-store-vs-labeled-property-graph-difference/)
2. Adding a name to the triple makes a "quad store" or named graph. [source](https://en.wikipedia.org/wiki/Triplestore#Related_database_types)

### Cayley
1. Cayley is a quad store that supports multiple storage backends.  It supports multiple query languages for traversing and filtering the named graphs formed by its quads, and it has associated tooling such as a CLI, HTTP server, and so on.

### Gizmo
1. A [Gremlin/TinkerPop](http://tinkerpop.apache.org/)-inspired query language for Cayley.  Looks a lot like JavaScript, the syntax is documented [here](https://github.com/cayleygraph/cayley/blob/master/docs/GizmoAPI.md#graphv).

### g.V()
1. For Gremlin/TinkerPop, [g.V() returns a list of all the vertices in the graph](http://tinkerpop.apache.org/docs/3.3.3/tutorials/gremlins-anatomy/#_graphtraversalsource)
2. `.v()` is for "Vertex" in Gizmo, and it is used like `pathObject = graph.Vertex([nodeId],[nodeId]...)` (see [[path|#path]])

### inbound/outbound predicate
1. Inbound/outbound refers to the direction of a relation via a predicate.  In the case of the triple "A follows B", "follows" is an outbound predicate for `A` and an inbound predicate for `B`.    
In/out predicates can be expressed in a query language, for example using the format `resultSet = subject.out(predicate)` to discover matching `Object`s.  In the case of the triple "A follows B", `A.out(“follows”)` would return a set of nodes which contains `B`.  An excellent example of this sort of query format is given in the Gremlin/TinkerPop homepage example:
```js
What are the names of projects that were created by two friends?
    g.V().match(
      as(“a”).out(“knows”).as(“b”),
      as(“a”).out(“created”).as(“c”),
      as(“b”).out(“created”).as(“c”),
      as(“c”).in(“created”).count().is(2)).
        select(“c”).by(“name”)
```

### direction
1. Direction specifies a node's position within a quad. [source](https://godoc.org/github.com/cayleygraph/cayley/quad#Direction)
```go
const (
    Any Direction = iota
    Subject
    Predicate
    Object
    Label
)
```
2. Direction is passed to the `Get` method of a quad to access one of its four parts, see [quad.Get(d Direction) Value](https://godoc.org/github.com/cayleygraph/cayley/quad#Quad.Get)
3. The term "Direction" comes about from the concept of traversing a graph. Take for example the triple `{A, follows, B}` and supposing you "select" the predicate `follows`. Now you want to traverse the graph, so you move in the `Object` direction, and you now have `B` selected. Whereas the high-level [path](#path) abstraction for queries uses inbound/outbound predicates to represent movement on the graph, the bottom-level [iterator](#iterator) mechanic uses Direction.

### path
1. Paths are just a set of helpers to build a query, but they are not that good for building something more complex. You can try using [Shapes](#shape) for this - it will give you a full control of what the query actually does. [source](https://discourse.cayley.io/t/a-variety-of-questions/1183/2)
2. Path represents either a morphism (a pre-defined path stored for later use), or a concrete path, consisting of a morphism and an underlying QuadStore. [source](https://godoc.org/github.com/cayleygraph/cayley/graph/path#Path)
3. Underlying code:
  ```go
  type Path struct {
      stack       []morphism
      qs          graph.QuadStore
      baseContext pathContext
  }

  type morphism struct {
      IsTag    bool
      Reversal func(*pathContext) (morphism, *pathContext)
      Apply    applyMorphism
      tags     []string
  }

  type applyMorphism func(shape.Shape, *pathContext) (shape.Shape, *pathContext)
  ```
	So, as previously stated, the [path](https://godoc.org/github.com/cayleygraph/cayley/graph/path) package is just helper methods on top of the [shape](https://godoc.org/github.com/cayleygraph/cayley/graph/shape) package.

### morphism
1. Morphism is basically a path that is not attached to any particular quadstore or a particular starting point in the graph. Morphisms are meant to be used as a query part that can be applied to other queries to follow a path specified in the Morphism.  
A good example will be a `FollowRecursive` function that will apply a single morphism multiple times to get to all nodes that can be traversed recursively. [source](https://discourse.cayley.io/t/a-variety-of-questions/1183/2)

### iterator
1. So a graph query is roughly represented as a tree of iterators – things
that implement graph.Iterator. An iterator is (loosely) a stand-in for a
set of things that match a particular portion of the graph. [source](https://discourse.cayley.io/t/7-7-14-question-about-iterator/62)

### subiterator
1. So a graph query is roughly represented as a tree of iterators...Evaluation is merely calling Next() repeatedly on the iterator at the top of the tree. Subiterators, then, are the branches and leaves of the tree. [source](https://discourse.cayley.io/t/7-7-14-question-about-iterator/62)
2. Example of converting the Cayley-Gremlin-Go-API query `g.V(“B”).In(“follows”).All()` into an iterator tree:
	- **HasA** (subject) – gets the things in the subject field for:
		- **And** – the intersection of:
			- **LinksTo (predicate)** links that have the predicate of…:
				- Fixed iterator containing “follows” – … just the node “follows”.
			- **LinksTo (object)** links that have the object field of:
				- Fixed iterator containing “B” – … just the node “B”
               
### LinkTo iterator
1. A LinksTo takes a subiterator of nodes, and contains an iteration of links which "link to" those nodes in a given direction. ... Can be seen as the dual of the HasA iterator. [source](https://github.com/cayleygraph/cayley/blob/1f53d04893ea9b2736e9b2277bbba3f47b88711a/graph/iterator/linksto.go#L17)
	- Next()ing a LinksTo is straightforward -- iterate through all links to things in the subiterator, and then advance the subiterator, and do it again.
    	- To restate in pseudo-code; `results` is what would be returned in successive `Next()` calls:
        ```go
        var results []quad.Quad
        for _, node := range linkTo.subIterator {
        	for _, quad := range allQuads {
            	if quad.Get(linkTo.direction) == node {
                	results = append(results, quad)
                }
            }
        }
        ```
	- Contains()ing a LinksTo means, given a link, take the direction we care about and check if it's in our subiterator.
    	- To restate in pseudo-code:
        ```go
        for _, node := range linkTo.subIterator {
			if theLink.Get(linkTo.direction) == node {
            	return true
            }
        }
        return false
        ```
    
### HasA iterator
1. The HasA takes a subiterator of links, and acts as an iterator of nodes in the given direction. The name comes from the idea that a "link HasA subject" or a "link HasA predicate". [source](https://github.com/cayleygraph/cayley/blob/41bf496d9dfe622b385c1482789480df8b106472/graph/iterator/hasa.go#L17)
	- Next(), [We have a subiterator we can get a value from, and we can take that resultant quad, pull our direction out of it, and return that.](https://github.com/cayleygraph/cayley/blob/41bf496d9dfe622b385c1482789480df8b106472/graph/iterator/hasa.go#L206)
    ```go
    var results []quad.Value
    for _, quad := range hasA.subIterator {
    	results = append(results, quad.Get(hasA.direction))
    }
    ```
    - Contains()
    ```go
    for _, quad := range hasA.subIterator {
    	if quad.Get(hasA.direction) == theNode {
        	return true
        }
    }
    return false
    ```
    
### shape
1. Shape represent a query tree shape. [source](https://godoc.org/github.com/cayleygraph/cayley/graph/shape#Shape)
	```go
    type Shape interface {
        BuildIterator(qs graph.QuadStore) graph.Iterator
        Optimize(r Optimizer) (Shape, bool)
    }
    ```
2. This is the most interesting part of the query system - it describes how exactly the query looks like. ... This package also describes different query optimizations that are not specific to a backend. ... You can write a query using either Paths, Shapes or raw Iterators... [source](https://discourse.cayley.io/t/a-variety-of-questions/1183/2)
2. A Shape seems to be an abstract representation of a query, a level above Iterators and a level below Paths.  You can perform various operations on it (traverse inbound/outbound predicates, find unions and intersections, etc.) and most importantly build a tree of Iterators from it, which will do the mechanical act of processing quads to find results.

### token
1. In the context of a [quad store](https://godoc.org/github.com/cayleygraph/cayley/graph#QuadStore), a [graph.Value](https://godoc.org/github.com/cayleygraph/cayley/graph#Value).  However the backend wishes to implement it, a Value is merely a token to a quad or a node that the backing store itself understands, and the base iterators pass around.    
	For example, in a very traditional, graphd-style graph, these are int64s (guids of the primitives). In a very direct sort of graph, these could be pointers to structs, or merely quads, or whatever works best for the backing store.
    
### reification
1. “With reification, we create a metagraph on top of our graph that represents the statement that we have here. We create a new node that represents a statement and points at the subject...” [source](https://neo4j.com/blog/rdf-triple-store-vs-labeled-property-graph-difference/)
2. Reifying a relationship means viewing it as an entity. The purpose of reifying a relationship is to make it explicit, when additional information needs to be added to it.   
Viewing a relationship as an entity, one can say that the entity reifies the relationship. This is called reification of a relationship. Like any other entity, it must be an instance of an entity type. [source][1]


[1]: https://en.wikipedia.org/wiki/Reification_(computer_science)
