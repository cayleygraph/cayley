<p align="center">
  <img src="static/branding/cayley_side.png?raw=true" alt="Cayley" />
</p>
Cayley is an open-source graph inspired by the graph database behind [Freebase](http://freebase.com) and Google's [Knowledge Graph](http://www.google.com/insidesearch/features/search/knowledge.html).

Its goal is to be a part of the developer's toolbox where [Linked Data](http://linkeddata.org/) and graph-shaped data (semantic webs, social networks, etc) in general are concerned.

[![Build Status](https://travis-ci.org/google/cayley.png?branch=master)](https://travis-ci.org/google/cayley) [Trello Board](https://trello.com/b/KioFZb5O)

## Features

* Written in [Go](http://golang.org)
* Easy to get running (3 or 4 commands, below)
* RESTful API
  * or a REPL if you prefer
* Built-in query editor and visualizer
* Multiple query languages:
  * JavaScript, with a [Gremlin](http://gremlindocs.com/)-inspired\* graph object.
  * (simplified) [MQL](https://developers.google.com/freebase/v1/mql-overview), for Freebase fans
* Plays well with multiple backend stores:
  * [LevelDB](http://code.google.com/p/leveldb/)
  * [Bolt](http://github.com/boltdb/bolt)
  * [MongoDB](http://mongodb.org) for distributed stores
  * In-memory, ephemeral
* Modular design; easy to extend with new languages and backends
* Good test coverage
* Speed, where possible.

Rough performance testing shows that, on consumer hardware and an average disk, 134m quads in LevelDB is no problem and a multi-hop intersection query -- films starring X and Y -- takes ~150ms.

\* Note that while it's not exactly Gremlin, it certainly takes inspiration from that API. For this flavor, [see the documentation](docs/GremlinAPI.md).

## Getting Started

Grab the latest [release binary](http://github.com/google/cayley/releases) and extract it wherever you like.

If you prefer to build from source, see the documentation on the wiki at [How to start hacking on Cayley](https://github.com/google/cayley/wiki/How-to-start-hacking-on-Cayley)

`cd` to the directory and give it a quick test with:
```
./cayley repl --dbpath=testdata.nq
```

You should see a `cayley>` REPL prompt. Go ahead and give it a try:

```
// Simple math
cayley> 2 + 2

// JavaScript syntax
cayley> x = 2 * 8
cayley> x

// See all the entities in this small follow graph.
cayley> graph.Vertex().All()

// See only dani.
cayley> graph.Vertex("dani").All()

// See who dani follows.
cayley> graph.Vertex("dani").Out("follows").All()
```

**Sample Data**

For somewhat more interesting data, a sample of 30k movies from Freebase comes in the checkout.

```
./cayley repl --dbpath=30kmoviedata.nq.gz
```

To run the web frontend, replace the "repl" command with "http"

```
./cayley http --dbpath=30kmoviedata.nq.gz
```

And visit port 64210 on your machine, commonly [http://localhost:64210](http://localhost:64210)


## Running queries

The default environment is based on [Gremlin](http://gremlindocs.com/) and is simply a JavaScript environment. If you can write jQuery, you can query a graph.

You'll notice we have a special object, `graph` or `g`, which is how you can interact with the graph.

The simplest query is merely to return a single vertex. Using the 30kmoviedata.nq dataset from above, let's walk through some simple queries:

```javascript
// Query all vertices in the graph, limit to the first 5 vertices found.
graph.Vertex().GetLimit(5)

// Start with only one vertex, the literal name "Humphrey Bogart", and retrieve all of them.
graph.Vertex("Humphrey Bogart").All()

// `g` and `V` are synonyms for `graph` and `Vertex` respectively, as they are quite common.
g.V("Humphrey Bogart").All()

// "Humphrey Bogart" is a name, but not an entity. Let's find the entities with this name in our dataset.
// Follow links that are pointing In to our "Humphrey Bogart" node with the predicate "name".
g.V("Humphrey Bogart").In("name").All()

// Notice that "name" is a generic predicate in our dataset.
// Starting with a movie gives a similar effect.
g.V("Casablanca").In("name").All()

// Relatedly, we can ask the reverse; all ids with the name "Casablanca"
g.V().Has("name", "Casablanca").All()
```


You may start to notice a pattern here: with Gremlin, the query lines tend to:

Start somewhere in the graph | Follow a path | Run the query with "All" or "GetLimit"

g.V("Casablanca") | .In("name") | .All()

And these pipelines continue...

```javascript
// Let's get the list of actors in the film
g.V().Has("name","Casablanca")
  .Out("/film/film/starring").Out("/film/performance/actor")
  .Out("name").All()

// But this is starting to get long. Let's use a morphism -- a pre-defined path stored in a variable -- as our linkage

var filmToActor = g.Morphism().Out("/film/film/starring").Out("/film/performance/actor")

g.V().Has("name", "Casablanca").Follow(filmToActor).Out("name").All()

```

There's more in the JavaScript API Documentation, but that should give you a feel for how to walk around the graph.

## Disclaimer

Not a Google project, but created and maintained [by a Googler](http://github.com/barakmich), with permission from and assignment to Google, under the [Apache License, version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

## Contact

* Email list: [cayley-users at Google Groups](https://groups.google.com/forum/?hl=en#!forum/cayley-users)
* Twitter: [@cayleygraph](http://twitter.com/cayleygraph)
* IRC: [#cayley on Freenode](http://webchat.freenode.net/?channels=%23cayley&uio=d4)
