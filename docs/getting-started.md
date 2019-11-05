# Getting Started

This guide will take you through starting a graph based on provided data.

## Prerequisites

This tutorial requires you to be connected to **local Cayley installation**. For more information on installing Cayley locally, see [Install Cayley](installation.md).

## Start Cayley

```bash
cayley http
```

You should see:

```text
Cayley version: 0.7.5 (dev snapshot)
using backend "memstore"
listening on 127.0.0.1:64210, web interface at http://127.0.0.1:64210
```

You can now open the web-interface on: [localhost:64210](http://localhost:64210/).

Cayley is configured by default to run in memory \(That's what `backend memstore` means\). To change the configuration see the documentation for [Configuration File](configuration.md) or run `cayley http --help`.

For more information about the UI see: [UI Overview](ui-overview.md)

## Run with sample data

### Download sample data

[Sample Data](https://github.com/cayleygraph/cayley/raw/master/data/30kmoviedata.nq.gz)

### Run Cayley

```bash
cayley http --load 30kmoviedata.nq.gz
```

## Query Data

Using the 30kmoviedata.nq dataset from above, let's walk through some simple queries:

### Query all vertices in the graph

To select all vertices in the graph call, limit to 5 first results. `g` and `V` are synonyms for `graph` and `Vertex` respectively, as they are quite common.

```javascript
g.V().getLimit(5);
```

### Match a property of a vertex

Find vertex with property "Humphrey Bogart"

```javascript
g.V()
  .has("<name>", "Humphrey Bogart")
  .all();
```

You may start to notice a pattern here: with Gizmo, the query lines tend to:

Start somewhere in the graph \| Follow a path \| Run the query with "all" or "getLimit"

### Match a complex path

Get the list of actors in the film

```javascript
g.V()
  .has("<name>", "Casablanca")
  .out("</film/film/starring>")
  .out("</film/performance/actor>")
  .out("<name>")
  .all();
```

### Match

This is starting to get long. Let's use a Morphism, a pre-defined path stored in a variable, as our linkage

```javascript
var filmToActor = g
  .Morphism()
  .out("</film/film/starring>")
  .out("</film/performance/actor>");

g.V()
  .has("<name>", "Casablanca")
  .follow(filmToActor)
  .out("<name>")
  .all();
```

To learn more about querying see [Gizmo Documentation](gizmoapi.md)

