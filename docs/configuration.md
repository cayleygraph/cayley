# Configuration

Cayley can be configured using configuration file written in YAML / JSON or by passing flags to the command line. By default. All command line flags take precedence over the configuration file.

* [Recommended Configuration](configuration.md#Recommended-Configuration)
* [Configuration Options](configuration.md#Configuration-Options)
  * [Store](configuration.md#Store)
  * [Per-Store Options](configuration.md#Per-Store-Options)
  * [Query](configuration.md#Query)
  * [Load](configuration.md#Load)
* [Configuration File Location](configuration.md#Configuration-File-Location)

## Recommended Configuration

By default, Cayley is using the `memstore` store. `memstore` works best for datasets that can fit into the memory of the machine and workloads which doesn't require persistency. For large datasets and/or workloads with require persistency it is recommended to use the `bolt` store.

## Configuration Options

### Store

#### **`store.backend`**

* Type: String
* Default: `"memory"`

Determines the type of the underlying database. Options include:

* `memstore`: An in-memory store, based on an initial N-Quads file. Loses all changes when the process exits.

**Key-Value backends**

* `btree`: An in-memory store, used mostly to quickly verify KV backend functionality.
* `leveldb`: A persistent on-disk store backed by [LevelDB](https://github.com/google/leveldb).
* `bolt`: Stores the graph data on-disk in a [Bolt](https://github.com/boltdb/bolt) file. Uses more disk space and memory than LevelDB for smaller stores, but is often faster to write to and comparable for large ones, with faster average query times.

**NoSQL backends**

Slower, as it incurs network traffic, but multiple Cayley instances can disappear and reconnect at will, across a potentially horizontally-scaled store.

* `mongo`: Stores the graph data and indices in a [MongoDB](https://www.mongodb.com/) instance.
* `elastic`: Stores the graph data and indices in a [ElasticSearch](https://www.elastic.co/products/elasticsearch) instance.
* `couch`: Stores the graph data and indices in a [CouchDB](http://couchdb.apache.org/) instance.
* `pouch`: Stores the graph data and indices in a [PouchDB](https://pouchdb.com/). Requires building with [GopherJS](https://github.com/gopherjs/gopherjs).

**SQL backends**

* `postgres`: Stores the graph data and indices in a [PostgreSQL](https://www.postgresql.org) instance.
* `cockroach`: Stores the graph data and indices in a [CockroachDB](https://www.cockroachlabs.com/product/cockroachdb/) cluster.
* `mysql`: Stores the graph data and indices in a [MySQL](https://www.mysql.com/) or [MariaDB](https://mariadb.org/) instance.
* `sqlite`: Stores the graph data and indices in a [SQLite](https://www.sqlite.org) database.

#### **`store.address`**

* Type: String
* Default: ""
* Alias: `store.path`

Where does the database actually live? Dependent on the type of database. For each datastore:

* `memstore`: Path parameter is not supported.
* `leveldb`: Directory to hold the LevelDB database files.
* `bolt`: Path to the persistent single Bolt database file.
* `mongo`: "hostname:port" of the desired MongoDB server. More options can be provided in [mgo](https://godoc.org/github.com/globalsign/mgo#Dial) address format.
* `elastic`: `http://host:port` of the desired ElasticSearch server.
* `couch`: `http://user:pass@host:port/dbname` of the desired CouchDB server.
* `postgres`,`cockroach`: `postgres://[username:password@]host[:port]/database-name?sslmode=disable` of the PostgreSQL database and credentials. Sslmode is optional. More option available on [pq](https://godoc.org/github.com/lib/pq) page.
* `mysql`: `[username:password@]tcp(host[:3306])/database-name` of the MqSQL database and credentials. More option available on [driver](https://github.com/go-sql-driver/mysql#dsn-data-source-name) page.
* `sqlite`: `filepath` of the SQLite database. More options available on [driver](https://github.com/mattn/go-sqlite3#connection-string) page.

#### **`store.read_only`**

* Type: Boolean
* Default: false

If true, disables the ability to write to the database using the HTTP API \(will return a 400 for any write request\). Useful for testing or instances that shouldn't change.

#### **`store.options`**

* Type: Object

See Per-Database Options, below.

### Per-Store Options

The `store.options` object in the main configuration file contains any of these following options that change the behavior of the datastore.

#### Memory

No special options.

#### LevelDB

**write\_buffer\_mb**

* Type: Integer
* Default: 20

The size in MiB of the LevelDB write cache. Increasing this number allows for more/faster writes before syncing to disk. Default is 20, for large loads, a recommended value is 200+.

**cache\_size\_mb**

* Type: Integer
* Default: 2

The size in MiB of the LevelDB block cache. Increasing this number uses more memory to maintain a bigger cache of quad blocks for better performance.

#### Bolt

**nosync**

* Type: Boolean
* Default: false

Optionally disable syncing to disk per transaction. Nosync being true means much faster load times, but without consistency guarantees.

#### Mongo

**database\_name**

* Type: String
* Default: "cayley"

The name of the database within MongoDB to connect to. Manages its own collections and indices therein.

#### PostgreSQL

Postgres version 9.5 or greater is required.

**db\_fill\_factor**

* Type: Integer
* Default: 50

Amount of empty space as a percentage to leave in the database when creating a table and inserting rows. See [PostgreSQL CreateTable](http://www.postgresql.org/docs/current/static/sql-createtable.html).

**local\_optimize**

* Type: Boolean
* Default: true

Whether to skip checking quad store size.

Connection pooling options used to configure the Go sql connection. Go defaults will be used when not specified.

**maxopenconnections**

* Type: Integer
* Default: -1.

**maxidleconnections**

* Type: Integer
* Default: -1.

**connmaxlifetime**

* Type: String
* Default: "".

#### Per-Replication Options

The `replication_options` object in the main configuration file contains any of these following options that change the behavior of the replication manager.

### Query

#### **`timeout`**

* Type: Integer or String
* Default: 30

The maximum length of time the Javascript runtime should run until cancelling the query and returning a 408 Timeout. When timeout is an integer is is interpreted as seconds, when it is a string it is [parsed](http://golang.org/pkg/time/#ParseDuration) as a Go time.Duration. A negative duration means no limit.

### Load

#### **`load.ignore_missing`**

* Type: Boolean
* Default: false

Optionally ignore missing quad on delete.

#### **`load.ignore_duplicate`**

* Type: Boolean
* Default: false

Optionally ignore duplicated quad on add.

#### **`load.batch`**

* Type: Integer
* Default: 10000

The number of quads to buffer from a loaded file before writing a block of quads to the database. Larger numbers are good for larger loads.

## Configuration File Location

Cayley looks in the following locations for the configuration file \(named `cayley.yml` or `cayley.json`\):

* Command line flag
* The environment variable `$CAYLEY_CFG`
* Current directory
* `$HOME/.cayley/`
* `/etc/`

