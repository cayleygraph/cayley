# Configuration File

## Overview

Cayley expects, in the usual case, to be run with a configuration file, though it can also be run purely through configuration flags. The configuration file contains a JSON object with any of the documented parameters.

Cayley looks in the following locations for the configuration file

  * Command line flag
  * The environment variable $CAYLEY_CFG
  * /etc/cayley.cfg

All command line flags take precedence over the configuration file.

## Main Options

#### **`database`**

  * Type: String
  * Default: "mem"

  Determines the type of the underlying database. Options include:

  * `mem`: An in-memory store, based on an initial N-Quads file. Loses all changes when the process exits.
  * `leveldb`: A persistent on-disk store backed by [LevelDB](http://code.google.com/p/leveldb/).
  * `bolt`: Stores the graph data on-disk in a [Bolt](http://github.com/boltdb/bolt) file. Uses more disk space and memory than LevelDB for smaller stores, but is often faster to write to and comparable for large ones, with faster average query times.
  * `mongo`: Stores the graph data and indices in a [MongoDB](http://mongodb.org) instance. Slower, as it incurs network traffic, but multiple Cayley instances can disappear and reconnect at will, across a potentially horizontally-scaled store.

#### **`db_path`**

  * Type: String
  * Default: "/tmp/testdb"

  Where does the database actually live? Dependent on the type of database. For each datastore:

  * `mem`: Path to a quad file to automatically load.
  * `leveldb`: Directory to hold the LevelDB database files.
  * `bolt`: Path to the persistent single Bolt database file.
  * `mongo`: "hostname:port" of the desired MongoDB server.

#### **`listen_host`**

  * Type: String
  * Default: "127.0.0.1"

  The hostname or IP address for Cayley's HTTP server to listen on. Defaults to all interfaces.

#### **`listen_port`**

  * Type: String
  * Default: "64210"

  The port for Cayley's HTTP server to listen on.

#### **`read_only`**

  * Type: Boolean
  * Default: false

  If true, disables the ability to write to the database using the HTTP API (will return a 400 for any write request). Useful for testing or instances that shouldn't change.

#### **`load_size`**

  * Type: Integer
  * Default: 10000

  The number of quads to buffer from a loaded file before writing a block of quads to the database. Larger numbers are good for larger loads.

#### **`db_options`**

  * Type: Object

  See Per-Database Options, below.

## Language Options

#### **`timeout`**

  * Type: Integer or String
  * Default: 30

The maximum length of time the Javascript runtime should run until cancelling the query and returning a 408 Timeout. When timeout is an integer is is interpretted as seconds, when it is a string it is [parsed](http://golang.org/pkg/time/#ParseDuration) as a Go time.Duration. A negative duration means no limit.

## Per-Database Options

The `db_options` object in the main configuration file contains any of these following options that change the behavior of the datastore.

### Memory

No special options.

### LevelDB

#### **`write_buffer_mb`**

  * Type: Integer
  * Default: 20

The size in MiB of the LevelDB write cache. Increasing this number allows for more/faster writes before syncing to disk. Default is 20, for large loads, a recommended value is 200+.

#### **`cache_size_mb`**

  * Type: Integer
  * Default: 2

The size in MiB of the LevelDB block cache. Increasing this number uses more memory to maintain a bigger cache of quad blocks for better performance.

### Bolt

#### **`nosync`**

  * Type: Boolean
  * Default: false

Optionally disable syncing to disk per transaction. Nosync being true means much faster load times, but without consistency guarantees.

### Mongo


#### **`database_name`**

  * Type: String
  * Default: "cayley"

The name of the database within MongoDB to connect to. Manages its own collections and indicies therein.
