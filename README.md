<p align="center">
  <img src="static/branding/cayley_side.png?raw=true" alt="Cayley" />
</p>

Cayley is an open-source graph inspired by the graph database behind [Freebase](http://freebase.com) and Google's [Knowledge Graph](https://en.wikipedia.org/wiki/Knowledge_Graph).

Its goal is to be a part of the developer's toolbox where [Linked Data](http://linkeddata.org/) and graph-shaped data (semantic webs, social networks, etc) in general are concerned.

[![Build Status](https://travis-ci.com/cayleygraph/cayley.svg?branch=master)](https://travis-ci.com/cayleygraph/cayley)
[![Container Repository](https://img.shields.io/docker/cloud/build/cayleygraph/cayley "Container Repository")](https://hub.docker.com/r/cayleygraph/cayley)

[![Get it from the Snap Store](https://snapcraft.io/static/images/badges/en/snap-store-white.svg)](https://snapcraft.io/cayley)

## Features

* Community driven
* Written in [Go](https://golang.org)
  * can be used as a Go library
* Easy to get running (3 or 4 commands)
* RESTful API
  * or a REPL if you prefer
* Built-in query editor and visualizer
* Multiple query languages:
  * [Gizmo](./docs/GizmoAPI.md) - a JavaScript, with a [Gremlin](http://gremlindocs.com/)-inspired\* graph object.
  * [GraphQL](./docs/GraphQL.md)-inspired\* query language.
  * (simplified) [MQL](./docs/MQL.md), for [Freebase](https://en.wikipedia.org/wiki/Freebase) fans
* Plays well with multiple backend stores:
  * KVs: [Bolt](https://github.com/boltdb/bolt), [LevelDB](https://github.com/google/leveldb)
  * NoSQL: [MongoDB](https://www.mongodb.org), [ElasticSearch](https://www.elastic.co/products/elasticsearch), [CouchDB](http://couchdb.apache.org/)/[PouchDB](https://pouchdb.com/)
  * SQL: [PostgreSQL](http://www.postgresql.org), [CockroachDB](https://www.cockroachlabs.com), [MySQL](https://www.mysql.com), [SQLite](https://www.sqlite.org)
  * In-memory, ephemeral
* Modular design; easy to extend with new languages and backends
* Good test coverage
* Speed, where possible.

Rough performance testing shows that, on 2014 consumer hardware and an average disk, 134m quads in LevelDB is no problem and a multi-hop intersection query -- films starring X and Y -- takes ~150ms.


## Community

* Website: [cayley.io](https://cayley.io)
* Slack: [cayleygraph.slack.com](https://cayleygraph.slack.com) -- Invite [here](https://cayley-slackin.herokuapp.com/)
* Discourse list: [discourse.cayley.io](https://discourse.cayley.io) (Also acts as mailing list, enable mailing list mode)
* Twitter: [@cayleygraph](https://twitter.com/cayleygraph)

## Documentation

* See the [docs folder](docs/) for v0.7 docs.
