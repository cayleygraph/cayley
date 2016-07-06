
\* Note that while it's not exactly Gremlin, it certainly takes inspiration from that API. For this flavor, [see the documentation](GremlinAPI.md).

## Getting Started

Grab the latest [release binary](https://github.com/cayleygraph/cayley/releases) and extract it wherever you like.

If you prefer to build from source, see the documentation on the wiki at [How to start hacking on Cayley](https://github.com/cayleygraph/cayley/wiki/How-to-start-hacking-on-Cayley) or type
```
mkdir -p ~/cayley && cd ~/cayley
export GOPATH=`pwd`
export PATH=$PATH:~/cayley/bin
mkdir -p bin pkg src/github.com/google
cd src/github.com/google
git clone https://github.com/cayleygraph/cayley
cd cayley
go get github.com/tools/godep
godep restore
go build ./cmd/cayley
```

Then `cd` to the directory and give it a quick test with:
```
./cayley repl --dbpath=data/testdata.nq
```

To run the web frontend, replace the "repl" command with "http"
```
./cayley http --dbpath=data/testdata.nq
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

**Running the visualizer on the web frontend**

To run the visualizer: click on visualize and enter:

```
// Visualize who dani follows.
g.V("dani").Tag("source").Out("follows").Tag("target").All()
```
The visualizer expects to tag nodes as either "source" or "target."  Your source is represented as a blue node.
While your target is represented as an orange node.
The idea being that our node relationship goes from blue to orange (source to target).

**Sample Data**

For somewhat more interesting data, a sample of 30k movies from Freebase comes in the checkout.

```
./cayley repl --dbpath=data/30kmoviedata.nq.gz
```

To run the web frontend, replace the "repl" command with "http"

```
./cayley http --dbpath=data/30kmoviedata.nq.gz
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

## Running in a container

A container exposing the HTTP API of cayley is available.
To run the container one must first setup a data directory that contains the configuration file and optionally contains persistent files (i.e. a boltdb database file).

```
mkdir data
cp my_config.cfg data/cayley.cfg
docker run -v $PWD/data:/data -p 64321:64321 -d quay.io/barakmich/cayley
```
