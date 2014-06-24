<p align="center">
  <img src="static/branding/cayley_side.png?raw=true" alt="Cayley" />
</p>
Cayley is an open-source graph inspired by the graph database behind [Freebase](http://freebase.com) and Google's [Knowledge Graph](http://www.google.com/insidesearch/features/search/knowledge.html). 

Its goal is to be a part of the developer's toolbox where [Linked Data](http://linkeddata.org/) and graph-shaped data (semantic webs, social networks, etc) in general are concerned.


## Features

* Written in [Go](http://golang.org)
* Easy to get running (3 or 4 commands, below)
* RESTful API
  * or a REPL if you prefer
* Built-in query editor and visualizer
* Multiple query languages:
  * Javascript, with a [Gremlin](http://gremlindocs.com/)-inspired\* graph object.
  * (simplified) [MQL](https://developers.google.com/freebase/v1/mql-overview), for Freebase fans
* Plays well with multiple backend stores:
  * [LevelDB](http://code.google.com/p/leveldb/) for single-machine storage
  * [MongoDB](http://mongodb.org)
  * In-memory, ephemeral
* Modular design; easy to extend with new languages and backends
* Good test coverage
* Speed, where possible.

Rough performance testing shows that, on consumer hardware and an average disk, 134m triples in LevelDB is no problem and a multi-hop intersection query -- films starring X and Y -- takes ~150ms.

\* Note that while it's not exactly Gremlin, it certainly takes inspiration from that API. For this flavor, [see the documentation](docs/GremlinAPI.md).

## Building
Make sure you have the right packages installed. Mostly, this is just Go as a dependency, and different ways of pulling packages.

### Linux
**Ubuntu / Debian**

`sudo apt-get install golang git bzr mercurial make`

**RHEL / Fedora**

`sudo yum install golang git bzr mercurial make gcc`


**OS X**

[Homebrew](http://brew.sh) is the preferred method. 

`brew install bazaar mercurial git go`

**Clone and build**

Now you can clone the repository and build the project.

```bash
git clone **INSERT PATH HERE**
cd cayley
make deps
make
```

And the `cayley` binary will be built and ready.

Give it a quick test with:
``` ./cayley repl --dbpath=testdata.nt ```

For somewhat more interesting data, a sample of 30k movies from Freebase comes in the checkout. 

```
gzip -cd 30kmoviedata.nt.gz > 30kmovies.nt
./cayley repl --dbpath=30kmovies.nt
```

To run the web frontend, replace the "repl" command with "http" 

```
./cayley http --dbpath=30kmovies.nt
```

And visit port 64210 on your machine, commonly [http://localhost:64210](http://localhost:64210)


## Running queries

The default environment is based on [Gremlin](http://gremlindocs.com/) and is simply a Javascript environment. If you can write jQuery, you can query a graph.

You'll notice we have a special object, `graph` or `g`, which is how you can interact with the graph. 

The simplest query is merely to return a single vertex. Using the 30kmovies.nt dataset from above, let's walk through some simple queries:

```javascript
// Query all vertices in the graph, limit to the first 5 vertices found.
graph.Vertex().Limit(5)

// Start with only one vertex, the literal name "Humphrey Bogart", and retreive all of them.
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

Start somewhere in the graph | Follow a path | Run the query with "All" or "Limit"

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

There's more in the Javascript API Documentation, but that should give you a feel for how to walk around the graph.

## Disclaimer

Not a Google project, but created and maintained [by a Googler](http://github.com/barakmich), with permission from and assignment to Google, under the [Apache License, version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

## Contact

* Email list: [cayley-users at Google Groups](https://groups.google.com/forum/?hl=en#!forum/cayley-users)
* Twitter: [@cayleygraph](http://twitter.com/cayleygraph)
* IRC: [#cayley on Freenode](http://webchat.freenode.net/?channels=%23cayley&uio=d4)
