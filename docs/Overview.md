# Overview

## Getting Started

This guide will take you through starting a persistent graph based on the provided data, with some hints for each backend.

Grab the latest [release binary](http://github.com/google/cayley/releases) and extract it wherever you like.

If you prefer to build from source, see the documentation on the wiki at [How to start hacking on Cayley](https://github.com/google/cayley/wiki/How-to-start-hacking-on-Cayley)

### Initialize A Graph

Now that Cayley is built, let's create our database. `init` is the subcommand to set up a database and the right indices.

You can set up a full [configuration file](/docs/Configuration.md) if you'd prefer, but it will also work from the command line.

Examples for each backend:

  * `leveldb`:  `./cayley init --db=leveldb --dbpath=/tmp/moviedb` -- where /tmp/moviedb is the path you'd like to store your data.
  * `bolt`:  `./cayley init --db=bolt --dbpath=/tmp/moviedb` -- where /tmp/moviedb is the filename where you'd like to store your data.
  * `mongo`: `./cayley init --db=mongo --dbpath="<HOSTNAME>:<PORT>"` -- where HOSTNAME and PORT point to your Mongo instance.

Those two options (db and dbpath) are always going to be present. If you feel like not repeating yourself, setting up a configuration file for your backend might be something to do now. There's an example file, `cayley.cfg.example` in the root directory.

You can repeat the `--db` and `--dbpath` flags from here forward instead of the config flag, but let's assume you created `cayley.cfg.overview`

### Load Data Into A Graph

First we load the data.

```bash
./cayley load --config=cayley.cfg.overview --quads=data/30kmoviedata.nq.gz
```

And wait. It will load. If you'd like to watch it load, you can run

```bash
./cayley load --config=cayley.cfg.overview --quads=data/30kmoviedata.nq.gz --alsologtostderr
```

And watch the log output go by.

### Connect a REPL To Your Graph

Now it's loaded. We can use Cayley now to connect to the graph. As you might have guessed, that command is:

```bash
./cayley repl --config=cayley.cfg.overview
```

Where you'll be given a `cayley>` prompt. It's expecting Gremlin/JS, but that can also be configured with a flag.

New nodes and links can be added with the following command:

```bash
cayley> :a subject predicate object label .
```

Removing links works similarly:

```bash
cayley> :d subject predicate object .
```

This is great for testing, and ultimately also for scripting, but the real workhorse is the next step.

### Serve Your Graph

Just as before:

```bash
./cayley http --config=cayley.cfg.overview
```

And you'll see a message not unlike

```bash
Cayley now listening on 127.0.0.1:64210
```

If you visit that address (often, [http://localhost:64210](http://localhost:64210)) you'll see the full web interface and also have a graph ready to serve queries via the [HTTP API](/docs/HTTP.md)

## UI Overview

### Sidebar

Along the side are the various actions or views you can take. From the top, these are:

* Run Query (run the query)
* Gremlin (a dropdown, to pick your query language)

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
  "source": "node1"
  "target": "node2"
},
{
  "source": "node1"
  "target": "node3"
},
]
```

Other keys are ignored. The upshot is that if you use the "Tag" functionality to add "source" and "target" tags, you can extract and quickly view subgraphs.
