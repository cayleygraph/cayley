# Quickstart as Application (an overview)

## Getting Started

This guide will take you through starting a persistent graph based on the provided data, with some hints for each backend.

Grab the latest [release binary](http://github.com/cayleygraph/cayley/releases) and extract it wherever you like.
If you have Docker installed you can check [guide](Container.md) for running Cayley in container.

If you prefer to build from source, see [Contributing.md](Contributing.md) which has instructions. 

### Quick preview ###
If you downloaded the correct binary the fastest way to have a peak into Cayley is to load one of the example data file in the ./data directory, and query them by the web interface.

```bash
./cayley http -i ./data/30kmoviedata.nq.gz -d memstore --host=:64210
```

```
Cayley version: x.y.z
using backend "memstore"
loaded "./data/30kmoviedata.nq.gz"
listening on :64210, web interface at http://localhost:64210
```

You can now open the web-interface on: [localhost:64210](http://localhost:64210/)

Or you can directly configure a backend storage engine like defined below and create your own graph.

### Initialize A Graph

Now that Cayley is downloaded (or built), let's create our database. `init` is the subcommand to set up a database and the right indices.

You can set up a full [configuration file](Configuration.md) if you'd prefer, but it will also work from the command line.

Examples for each backend can be found in `store.address` format from [config file](Configuration.md).

Those two options (db and dbpath) are always going to be present. If you feel like not repeating yourself, setting up a configuration file for your backend might be something to do now. There's an example file, `cayley_example.yml` in the root directory.

You can repeat the `--db (-i)` and `--dbpath (-a)` flags from here forward instead of the config flag, but let's assume you created `cayley_overview.yml`

Note: when you specify parameters in the config file the config flags (command line arguments) are ignored.

### Load Data Into A Graph

After the database is initialized we load the data.

```bash
./cayley load -c cayley_overview.yml -i data/testdata.nq
```

And wait. It will load. If you'd like to watch it load, you can run

```bash
./cayley load -c cayley_overview.yml -i data/testdata.nq --alsologtostderr=true
```

And watch the log output go by.

If you plan to import a large dataset into Cayley and try multiple backends, it makes sense to first convert the dataset
to Cayley-specific binary format by running:

```bash
./cayley conv -i dataset.nq.gz -o dataset.pq.gz
```

This will minimize parsing overhead on future imports and will compress dataset a bit better.

### Connect a REPL To Your Graph

Now it's loaded. We can use Cayley now to connect to the graph. As you might have guessed, that command is:

```bash
./cayley repl -c cayley_overview.yml
```

Where you'll be given a `cayley>` prompt. It's expecting Gizmo/JS, but that can also be configured with a flag.

New nodes and links can be added with the following command:

```bash
cayley> :a subject predicate object label .
```

Removing links works similarly:

```bash
cayley> :d subject predicate object .
```

This is great for testing, and ultimately also for scripting, but the real workhorse is the next step.

Go ahead and give it a try:

```
// Simple math
cayley> 2 + 2

// JavaScript syntax
cayley> x = 2 * 8
cayley> x

// See all the entities in this small follow graph.
cayley> graph.Vertex().All()

// See only dani.
cayley> graph.Vertex("<dani>").All()

// See who dani follows.
cayley> graph.Vertex("<dani>").Out("<follows>").All()
```


### Serve Your Graph

Just as before:

```bash
./cayley http -c cayley_overview.yml
```

And you'll see a message not unlike

```bash
listening on :64210, web interface at http://localhost:64210
```

If you visit that address (often, [http://localhost:64210](http://localhost:64210)) you'll see the full web interface and also have a graph ready to serve queries via the [HTTP API](HTTP.md)

#### Access from other machines ####
When you want to reach the API or UI from another machine in the network you need to specify the host argument:
```bash
./cayley http --config=cayley.cfg.overview --host=0.0.0.0:64210
```
This makes it listen on all interfaces. You can also give it the specific the IP address you want Cayley to bind to. 

**Warning**: for security reasons you might not want to do this on a public accessible machine. 


## UI Overview

### Sidebar

Along the side are the various actions or views you can take. From the top, these are:

* Run Query (run the query)
* Gizmo (a dropdown, to pick your query language, MQL is the other)
  * [GizmoAPI.md](GizmoAPI.md): This is the one of the two query languages used either via the REPL or HTTP interface.
  * [MQL.md](MQL.md): The *other* query language the interfaces support. 

----

* Query (a request/response editor for the query language)
* Query Shape (a visualization of the shape of the final query. Does not execute the query.)
* Visualize  (runs a query and, if tagged correctly, gives a sigmajs view of the results)
* Write (an interface to write or remove individual quads or quad files)

----

* Documentation (this documentation)

### Visualize

To use the visualize function, emit, either through tags or JS post-processing, a set of JSON objects containing the keys `source` and `target`. These will be the links, and nodes will automatically be detected.

For example:

```javascript
[
{
  "source": "node1",
  "target": "node2"
},
{
  "source": "node1",
  "target": "node3"
},
]
```

Other keys are ignored. The upshot is that if you use the "Tag" functionality to add "source" and "target" tags, you can extract and quickly view subgraphs.

```
// Visualize who dani follows.
g.V("<dani>").Tag("source").Out("<follows>").Tag("target").All()
```
The visualizer expects to tag nodes as either "source" or "target."  Your source is represented as a blue node.
While your target is represented as an orange node.
The idea being that our node relationship goes from blue to orange (source to target).

----


**Sample Data**

For more interesting test data -- follow the same loading procedure as outlined above, but with "data/30kmoviedata.nq.gz"

## Running some more interesting queries

The simplest query is merely to return a single vertex. Using the 30kmoviedata.nq dataset from above, let's walk through some simple queries:

```javascript
// Query all vertices in the graph, limit to the first 5 vertices found.
graph.Vertex().GetLimit(5)

// Start with only one vertex, the literal name "Humphrey Bogart", and retrieve all of them.
graph.Vertex("Humphrey Bogart").All()

// `g` and `V` are synonyms for `graph` and `Vertex` respectively, as they are quite common.
g.V("Humphrey Bogart").All()

// "Humphrey Bogart" is a name, but not an entity. Let's find the entities with this name in our dataset.
// Follow links that are pointing In to our "Humphrey Bogart" node with the predicate "<name>".
g.V("Humphrey Bogart").In("<name>").All()

// Notice that "<name>" is a generic predicate in our dataset.
// Starting with a movie gives a similar effect.
g.V("Casablanca").In("<name>").All()

// Relatedly, we can ask the reverse; all ids with the name "Casablanca"
g.V().Has("<name>", "Casablanca").All()
```


You may start to notice a pattern here: with Gizmo, the query lines tend to:

Start somewhere in the graph | Follow a path | Run the query with "All" or "GetLimit"

g.V("Casablanca") | .In("<name>") | .All()

And these pipelines continue...

```javascript
// Let's get the list of actors in the film
g.V().Has("<name>","Casablanca")
  .Out("</film/film/starring>").Out("</film/performance/actor>")
  .Out("<name>").All()

// But this is starting to get long. Let's use a morphism -- a pre-defined path stored in a variable -- as our linkage

var filmToActor = g.Morphism().Out("</film/film/starring>").Out("</film/performance/actor>")

g.V().Has("<name>", "Casablanca").Follow(filmToActor).Out("<name>").All()

```

There's more in the JavaScript API Documentation, but that should give you a feel for how to walk around the graph.
