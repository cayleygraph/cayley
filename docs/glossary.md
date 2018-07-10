# Glossary of Terms


### triple
1. [a data entity composed of subject-predicate-object, like "Bob is 35" or "Bob knows Fred".](https://en.wikipedia.org/wiki/Triplestore)

### triplestore
1. [a purpose-built database for the storage and retrieval of triples...](https://en.wikipedia.org/wiki/Triplestore)

### RDF
1. [Resource Description Framework](https://en.wikipedia.org/wiki/Resource_Description_Framework), basically triples

### RDF store, quad store, named graph, semantic graph database
1. [Adding a name to the triple makes a "quad store" or named graph.](https://en.wikipedia.org/wiki/Triplestore#Related_database_types)
1. [...persisting RDF — storing it — became a thing, and these stores were called triple stores. Next they were called quad stores and included information about context and named graphs, then RDF stores, and most recently they call themselves “semantic graph database.” ](https://neo4j.com/blog/rdf-triple-store-vs-labeled-property-graph-difference/)

### quad
1. [Adding a name to the triple makes a "quad store" or named graph.](https://en.wikipedia.org/wiki/Triplestore#Related_database_types)
2. [this notion of quad...You can add context or extra values to triples that identifies them and makes it easy to define subgraphs, or named properties.](https://neo4j.com/blog/rdf-triple-store-vs-labeled-property-graph-difference/)
3. [where triples have the form](https://en.wikipedia.org/wiki/Named_graph#Named_graphs_and_quads) `{subject, predicate, object}`, [quads would have a form along the lines of](https://en.wikipedia.org/wiki/Named_graph#Named_graphs_and_quads) `{subject, predicate, object, context}`

### morphism
1. [Morphism is basically a path that is not attached to any particular quadstore or a particular starting point in the graph. Morphisms are meant to be used as a query part that can be applied to other queries to follow a path specified in the Morphism.  A good example will be a `FollowRecursive` function that will apply a single morphism multiple times to get to all nodes that can be traversed recursively.](https://discourse.cayley.io/t/a-variety-of-questions/1183/2)
    
### reification
1. [“With reification, we create a metagraph on top of our graph that represents the statement that we have here. We create a new node that represents a statement and points at the subject...”](https://neo4j.com/blog/rdf-triple-store-vs-labeled-property-graph-difference/)
2. [Reifying a relationship means viewing it as an entity. The purpose of reifying a relationship is to make it explicit, when additional information needs to be added to it.][1]
3. [Viewing a relationship as an entity, one can say that the entity reifies the relationship. This is called reification of a relationship. Like any other entity, it must be an instance of an entity type.][1] 


[1]: https://en.wikipedia.org/wiki/Reification_(computer_science)