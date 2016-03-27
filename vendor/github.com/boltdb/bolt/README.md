Bolt [![Build Status](https://drone.io/github.com/boltdb/bolt/status.png)](https://drone.io/github.com/boltdb/bolt/latest) [![Coverage Status](https://coveralls.io/repos/boltdb/bolt/badge.png?branch=master)](https://coveralls.io/r/boltdb/bolt?branch=master) [![GoDoc](https://godoc.org/github.com/boltdb/bolt?status.png)](https://godoc.org/github.com/boltdb/bolt) ![Version](http://img.shields.io/badge/version-1.0-green.png)
====

Bolt is a pure Go key/value store inspired by [Howard Chu's][hyc_symas] and
the [LMDB project][lmdb]. The goal of the project is to provide a simple,
fast, and reliable database for projects that don't require a full database
server such as Postgres or MySQL.

Since Bolt is meant to be used as such a low-level piece of functionality,
simplicity is key. The API will be small and only focus on getting values
and setting values. That's it.

[hyc_symas]: https://twitter.com/hyc_symas
[lmdb]: http://symas.com/mdb/


## Project Status

Bolt is stable and the API is fixed. Full unit test coverage and randomized
black box testing are used to ensure database consistency and thread safety.
Bolt is currently in high-load production environments serving databases as
large as 1TB. Many companies such as Shopify and Heroku use Bolt-backed
services every day.


## Getting Started

### Installing

To start using Bolt, install Go and run `go get`:

```sh
$ go get github.com/boltdb/bolt/...
```

This will retrieve the library and install the `bolt` command line utility into
your `$GOBIN` path.


### Opening a database

The top-level object in Bolt is a `DB`. It is represented as a single file on
your disk and represents a consistent snapshot of your data.

To open your database, simply use the `bolt.Open()` function:

```go
package main

import (
	"log"

	"github.com/boltdb/bolt"
)

func main() {
	// Open the my.db data file in your current directory.
	// It will be created if it doesn't exist.
	db, err := bolt.Open("my.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	...
}
```

Please note that Bolt obtains a file lock on the data file so multiple processes
cannot open the same database at the same time. Opening an already open Bolt
database will cause it to hang until the other process closes it. To prevent
an indefinite wait you can pass a timeout option to the `Open()` function:

```go
db, err := bolt.Open("my.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
```


### Transactions

Bolt allows only one read-write transaction at a time but allows as many
read-only transactions as you want at a time. Each transaction has a consistent
view of the data as it existed when the transaction started.

Individual transactions and all objects created from them (e.g. buckets, keys)
are not thread safe. To work with data in multiple goroutines you must start
a transaction for each one or use locking to ensure only one goroutine accesses
a transaction at a time. Creating transaction from the `DB` is thread safe.


#### Read-write transactions

To start a read-write transaction, you can use the `DB.Update()` function:

```go
err := db.Update(func(tx *bolt.Tx) error {
	...
	return nil
})
```

Inside the closure, you have a consistent view of the database. You commit the
transaction by returning `nil` at the end. You can also rollback the transaction
at any point by returning an error. All database operations are allowed inside
a read-write transaction.

Always check the return error as it will report any disk failures that can cause
your transaction to not complete. If you return an error within your closure
it will be passed through.


#### Read-only transactions

To start a read-only transaction, you can use the `DB.View()` function:

```go
err := db.View(func(tx *bolt.Tx) error {
	...
	return nil
})
```

You also get a consistent view of the database within this closure, however,
no mutating operations are allowed within a read-only transaction. You can only
retrieve buckets, retrieve values, and copy the database within a read-only
transaction.


#### Batch read-write transactions

Each `DB.Update()` waits for disk to commit the writes. This overhead
can be minimized by combining multiple updates with the `DB.Batch()`
function:

```go
err := db.Batch(func(tx *bolt.Tx) error {
	...
	return nil
})
```

Concurrent Batch calls are opportunistically combined into larger
transactions. Batch is only useful when there are multiple goroutines
calling it.

The trade-off is that `Batch` can call the given
function multiple times, if parts of the transaction fail. The
function must be idempotent and side effects must take effect only
after a successful return from `DB.Batch()`.

For example: don't display messages from inside the function, instead
set variables in the enclosing scope:

```go
var id uint64
err := db.Batch(func(tx *bolt.Tx) error {
	// Find last key in bucket, decode as bigendian uint64, increment
	// by one, encode back to []byte, and add new key.
	...
	id = newValue
	return nil
})
if err != nil {
	return ...
}
fmt.Println("Allocated ID %d", id)
```


#### Managing transactions manually

The `DB.View()` and `DB.Update()` functions are wrappers around the `DB.Begin()`
function. These helper functions will start the transaction, execute a function,
and then safely close your transaction if an error is returned. This is the
recommended way to use Bolt transactions.

However, sometimes you may want to manually start and end your transactions.
You can use the `Tx.Begin()` function directly but _please_ be sure to close the
transaction.

```go
// Start a writable transaction.
tx, err := db.Begin(true)
if err != nil {
    return err
}
defer tx.Rollback()

// Use the transaction...
_, err := tx.CreateBucket([]byte("MyBucket"))
if err != nil {
    return err
}

// Commit the transaction and check for error.
if err := tx.Commit(); err != nil {
    return err
}
```

The first argument to `DB.Begin()` is a boolean stating if the transaction
should be writable.


### Using buckets

Buckets are collections of key/value pairs within the database. All keys in a
bucket must be unique. You can create a bucket using the `DB.CreateBucket()`
function:

```go
db.Update(func(tx *bolt.Tx) error {
	b, err := tx.CreateBucket([]byte("MyBucket"))
	if err != nil {
		return fmt.Errorf("create bucket: %s", err)
	}
	return nil
})
```

You can also create a bucket only if it doesn't exist by using the
`Tx.CreateBucketIfNotExists()` function. It's a common pattern to call this
function for all your top-level buckets after you open your database so you can
guarantee that they exist for future transactions.

To delete a bucket, simply call the `Tx.DeleteBucket()` function.


### Using key/value pairs

To save a key/value pair to a bucket, use the `Bucket.Put()` function:

```go
db.Update(func(tx *bolt.Tx) error {
	b := tx.Bucket([]byte("MyBucket"))
	err := b.Put([]byte("answer"), []byte("42"))
	return err
})
```

This will set the value of the `"answer"` key to `"42"` in the `MyBucket`
bucket. To retrieve this value, we can use the `Bucket.Get()` function:

```go
db.View(func(tx *bolt.Tx) error {
	b := tx.Bucket([]byte("MyBucket"))
	v := b.Get([]byte("answer"))
	fmt.Printf("The answer is: %s\n", v)
	return nil
})
```

The `Get()` function does not return an error because its operation is
guarenteed to work (unless there is some kind of system failure). If the key
exists then it will return its byte slice value. If it doesn't exist then it
will return `nil`. It's important to note that you can have a zero-length value
set to a key which is different than the key not existing.

Use the `Bucket.Delete()` function to delete a key from the bucket.

Please note that values returned from `Get()` are only valid while the
transaction is open. If you need to use a value outside of the transaction
then you must use `copy()` to copy it to another byte slice.


### Iterating over keys

Bolt stores its keys in byte-sorted order within a bucket. This makes sequential
iteration over these keys extremely fast. To iterate over keys we'll use a
`Cursor`:

```go
db.View(func(tx *bolt.Tx) error {
	b := tx.Bucket([]byte("MyBucket"))
	c := b.Cursor()

	for k, v := c.First(); k != nil; k, v = c.Next() {
		fmt.Printf("key=%s, value=%s\n", k, v)
	}

	return nil
})
```

The cursor allows you to move to a specific point in the list of keys and move
forward or backward through the keys one at a time.

The following functions are available on the cursor:

```
First()  Move to the first key.
Last()   Move to the last key.
Seek()   Move to a specific key.
Next()   Move to the next key.
Prev()   Move to the previous key.
```

When you have iterated to the end of the cursor then `Next()` will return `nil`.
You must seek to a position using `First()`, `Last()`, or `Seek()` before
calling `Next()` or `Prev()`. If you do not seek to a position then these
functions will return `nil`.


#### Prefix scans

To iterate over a key prefix, you can combine `Seek()` and `bytes.HasPrefix()`:

```go
db.View(func(tx *bolt.Tx) error {
	c := tx.Bucket([]byte("MyBucket")).Cursor()

	prefix := []byte("1234")
	for k, v := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = c.Next() {
		fmt.Printf("key=%s, value=%s\n", k, v)
	}

	return nil
})
```

#### Range scans

Another common use case is scanning over a range such as a time range. If you
use a sortable time encoding such as RFC3339 then you can query a specific
date range like this:

```go
db.View(func(tx *bolt.Tx) error {
	// Assume our events bucket has RFC3339 encoded time keys.
	c := tx.Bucket([]byte("Events")).Cursor()

	// Our time range spans the 90's decade.
	min := []byte("1990-01-01T00:00:00Z")
	max := []byte("2000-01-01T00:00:00Z")

	// Iterate over the 90's.
	for k, v := c.Seek(min); k != nil && bytes.Compare(k, max) <= 0; k, v = c.Next() {
		fmt.Printf("%s: %s\n", k, v)
	}

	return nil
})
```


#### ForEach()

You can also use the function `ForEach()` if you know you'll be iterating over
all the keys in a bucket:

```go
db.View(func(tx *bolt.Tx) error {
	b := tx.Bucket([]byte("MyBucket"))
	b.ForEach(func(k, v []byte) error {
		fmt.Printf("key=%s, value=%s\n", k, v)
		return nil
	})
	return nil
})
```


### Nested buckets

You can also store a bucket in a key to create nested buckets. The API is the
same as the bucket management API on the `DB` object:

```go
func (*Bucket) CreateBucket(key []byte) (*Bucket, error)
func (*Bucket) CreateBucketIfNotExists(key []byte) (*Bucket, error)
func (*Bucket) DeleteBucket(key []byte) error
```


### Database backups

Bolt is a single file so it's easy to backup. You can use the `Tx.WriteTo()`
function to write a consistent view of the database to a writer. If you call
this from a read-only transaction, it will perform a hot backup and not block
your other database reads and writes. It will also use `O_DIRECT` when available
to prevent page cache trashing.

One common use case is to backup over HTTP so you can use tools like `cURL` to
do database backups:

```go
func BackupHandleFunc(w http.ResponseWriter, req *http.Request) {
	err := db.View(func(tx *bolt.Tx) error {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", `attachment; filename="my.db"`)
		w.Header().Set("Content-Length", strconv.Itoa(int(tx.Size())))
		_, err := tx.WriteTo(w)
		return err
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
```

Then you can backup using this command:

```sh
$ curl http://localhost/backup > my.db
```

Or you can open your browser to `http://localhost/backup` and it will download
automatically.

If you want to backup to another file you can use the `Tx.CopyFile()` helper
function.


### Statistics

The database keeps a running count of many of the internal operations it
performs so you can better understand what's going on. By grabbing a snapshot
of these stats at two points in time we can see what operations were performed
in that time range.

For example, we could start a goroutine to log stats every 10 seconds:

```go
go func() {
	// Grab the initial stats.
	prev := db.Stats()

	for {
		// Wait for 10s.
		time.Sleep(10 * time.Second)

		// Grab the current stats and diff them.
		stats := db.Stats()
		diff := stats.Sub(&prev)

		// Encode stats to JSON and print to STDERR.
		json.NewEncoder(os.Stderr).Encode(diff)

		// Save stats for the next loop.
		prev = stats
	}
}()
```

It's also useful to pipe these stats to a service such as statsd for monitoring
or to provide an HTTP endpoint that will perform a fixed-length sample.


## Resources

For more information on getting started with Bolt, check out the following articles:

* [Intro to BoltDB: Painless Performant Persistence](http://npf.io/2014/07/intro-to-boltdb-painless-performant-persistence/) by [Nate Finch](https://github.com/natefinch).
* [Bolt -- an embedded key/value database for Go](https://www.progville.com/go/bolt-embedded-db-golang/) by Progville


## Comparison with other databases

### Postgres, MySQL, & other relational databases

Relational databases structure data into rows and are only accessible through
the use of SQL. This approach provides flexibility in how you store and query
your data but also incurs overhead in parsing and planning SQL statements. Bolt
accesses all data by a byte slice key. This makes Bolt fast to read and write
data by key but provides no built-in support for joining values together.

Most relational databases (with the exception of SQLite) are standalone servers
that run separately from your application. This gives your systems
flexibility to connect multiple application servers to a single database
server but also adds overhead in serializing and transporting data over the
network. Bolt runs as a library included in your application so all data access
has to go through your application's process. This brings data closer to your
application but limits multi-process access to the data.


### LevelDB, RocksDB

LevelDB and its derivatives (RocksDB, HyperLevelDB) are similar to Bolt in that
they are libraries bundled into the application, however, their underlying
structure is a log-structured merge-tree (LSM tree). An LSM tree optimizes
random writes by using a write ahead log and multi-tiered, sorted files called
SSTables. Bolt uses a B+tree internally and only a single file. Both approaches
have trade offs.

If you require a high random write throughput (>10,000 w/sec) or you need to use
spinning disks then LevelDB could be a good choice. If your application is
read-heavy or does a lot of range scans then Bolt could be a good choice.

One other important consideration is that LevelDB does not have transactions.
It supports batch writing of key/values pairs and it supports read snapshots
but it will not give you the ability to do a compare-and-swap operation safely.
Bolt supports fully serializable ACID transactions.


### LMDB

Bolt was originally a port of LMDB so it is architecturally similar. Both use
a B+tree, have ACID semantics with fully serializable transactions, and support
lock-free MVCC using a single writer and multiple readers.

The two projects have somewhat diverged. LMDB heavily focuses on raw performance
while Bolt has focused on simplicity and ease of use. For example, LMDB allows
several unsafe actions such as direct writes for the sake of performance. Bolt
opts to disallow actions which can leave the database in a corrupted state. The
only exception to this in Bolt is `DB.NoSync`.

There are also a few differences in API. LMDB requires a maximum mmap size when
opening an `mdb_env` whereas Bolt will handle incremental mmap resizing
automatically. LMDB overloads the getter and setter functions with multiple
flags whereas Bolt splits these specialized cases into their own functions.


## Caveats & Limitations

It's important to pick the right tool for the job and Bolt is no exception.
Here are a few things to note when evaluating and using Bolt:

* Bolt is good for read intensive workloads. Sequential write performance is
  also fast but random writes can be slow. You can add a write-ahead log or
  [transaction coalescer](https://github.com/boltdb/coalescer) in front of Bolt
  to mitigate this issue.

* Bolt uses a B+tree internally so there can be a lot of random page access.
  SSDs provide a significant performance boost over spinning disks.

* Try to avoid long running read transactions. Bolt uses copy-on-write so
  old pages cannot be reclaimed while an old transaction is using them.

* Byte slices returned from Bolt are only valid during a transaction. Once the
  transaction has been committed or rolled back then the memory they point to
  can be reused by a new page or can be unmapped from virtual memory and you'll
  see an `unexpected fault address` panic when accessing it.

* Be careful when using `Bucket.FillPercent`. Setting a high fill percent for
  buckets that have random inserts will cause your database to have very poor
  page utilization.

* Use larger buckets in general. Smaller buckets causes poor page utilization
  once they become larger than the page size (typically 4KB).

* Bulk loading a lot of random writes into a new bucket can be slow as the
  page will not split until the transaction is committed. Randomly inserting
  more than 100,000 key/value pairs into a single new bucket in a single
  transaction is not advised.

* Bolt uses a memory-mapped file so the underlying operating system handles the
  caching of the data. Typically, the OS will cache as much of the file as it
  can in memory and will release memory as needed to other processes. This means
  that Bolt can show very high memory usage when working with large databases.
  However, this is expected and the OS will release memory as needed. Bolt can
  handle databases much larger than the available physical RAM.

* Because of the way pages are laid out on disk, Bolt cannot truncate data files
  and return free pages back to the disk. Instead, Bolt maintains a free list
  of unused pages within its data file. These free pages can be reused by later
  transactions. This works well for many use cases as databases generally tend
  to grow. However, it's important to note that deleting large chunks of data
  will not allow you to reclaim that space on disk.

  For more information on page allocation, [see this comment][page-allocation].

[page-allocation]: https://github.com/boltdb/bolt/issues/308#issuecomment-74811638


## Other Projects Using Bolt

Below is a list of public, open source projects that use Bolt:

* [Operation Go: A Routine Mission](http://gocode.io) - An online programming game for Golang using Bolt for user accounts and a leaderboard.
* [Bazil](https://github.com/bazillion/bazil) - A file system that lets your data reside where it is most convenient for it to reside.
* [DVID](https://github.com/janelia-flyem/dvid) - Added Bolt as optional storage engine and testing it against Basho-tuned leveldb.
* [Skybox Analytics](https://github.com/skybox/skybox) - A standalone funnel analysis tool for web analytics.
* [Scuttlebutt](https://github.com/benbjohnson/scuttlebutt) - Uses Bolt to store and process all Twitter mentions of GitHub projects.
* [Wiki](https://github.com/peterhellberg/wiki) - A tiny wiki using Goji, BoltDB and Blackfriday.
* [ChainStore](https://github.com/nulayer/chainstore) - Simple key-value interface to a variety of storage engines organized as a chain of operations.
* [MetricBase](https://github.com/msiebuhr/MetricBase) - Single-binary version of Graphite.
* [Gitchain](https://github.com/gitchain/gitchain) - Decentralized, peer-to-peer Git repositories aka "Git meets Bitcoin".
* [event-shuttle](https://github.com/sclasen/event-shuttle) - A Unix system service to collect and reliably deliver messages to Kafka.
* [ipxed](https://github.com/kelseyhightower/ipxed) - Web interface and api for ipxed.
* [BoltStore](https://github.com/yosssi/boltstore) - Session store using Bolt.
* [photosite/session](http://godoc.org/bitbucket.org/kardianos/photosite/session) - Sessions for a photo viewing site.
* [LedisDB](https://github.com/siddontang/ledisdb) - A high performance NoSQL, using Bolt as optional storage.
* [ipLocator](https://github.com/AndreasBriese/ipLocator) - A fast ip-geo-location-server using bolt with bloom filters.
* [cayley](https://github.com/google/cayley) - Cayley is an open-source graph database using Bolt as optional backend.
* [bleve](http://www.blevesearch.com/) - A pure Go search engine similar to ElasticSearch that uses Bolt as the default storage backend.
* [tentacool](https://github.com/optiflows/tentacool) - REST api server to manage system stuff (IP, DNS, Gateway...) on a linux server.
* [SkyDB](https://github.com/skydb/sky) - Behavioral analytics database.
* [Seaweed File System](https://github.com/chrislusf/weed-fs) - Highly scalable distributed key~file system with O(1) disk read.
* [InfluxDB](http://influxdb.com) - Scalable datastore for metrics, events, and real-time analytics.

If you are using Bolt in a project please send a pull request to add it to the list.
