# TODOs

## Short Term

### Client modules

In Python, Node.js, the usual suspects. Even cooler would be a node.js/Gremlin bridge that gave you the graph object.

### Response wrapper details

Query time, statistics, that sort of thing.

### Better run-iterator centralization

It's everywhere now, with subtly different semantics. Unify and do cool things (like abort).

### More test coverage
Always good. Break out test_utils and compare text and Javascript outputs.

### More documentation
Also always good.

### Anything marked "TODO" in the code.
Usually something that should be taken care of.

### Bootstraps
Start discussing bootstrap quads, things that make the database self-describing, if they exist (though they need not). Talk about sameAs and indexing and type systems and whatnot.

### Better surfacing of Label
It exists, it's indexed, but it's basically useless right now

### Optimize HasA Iterator
There are some simple optimizations that can be done there. And was the first one to get right, this is the next one.
A simple example is just to convert the HasA to a fixed (next them out) if the subiterator size is guessable and small.

### Gremlin features

#### Mid-query Limit
A way to limit the number of subresults at a point, without even running the query. Essentially, much as GetLimit() does for the end, be able to do the same in between

#### "Up" and "Down" traversals
Getting to the predicates from a node, or the nodes from a predicate, or some odd combinations thereof. Ditto for label.

#### Value comparison
Expose the value-comparison iterator in the language

### MQL features
See also bootstrapping. Things like finding "name" predicates, and various schema or type enforcement.

An important failure of MQL before was that it was never well-specified. Let's not fall in that trap again, and be able to document what everything means.

### New Iterators

#### Limit Iterator
The necessary component to make mid-query limit work. Acts as a limit on Next(), a passthrough on Contains(), and a limit on NextPath()

## Medium Term

### Direct JSON-LD loading
  Because it's useful markup, and JSON is easy for Go to deal with. The NQuads library is nicely self-contained, there's no reason more formats can't be supported.

### Value indexing
  Since I have value comparison. It works, it's just not fast today. That could be improved.

### AppEngine (Datastore) Backend
  Hopefully easy now that the AppEngine shim exists. Questionably fast.

### Postgres Backend
  It'd be nice to run on SQL as well. It's a big why not?
#### Generalist layout
  Notionally, this is a simple quad table with a number of indicies. Iterators and iterator optimization (ie, rewriting SQL queries) is the 'fun' part
#### "Short Schema" Layout?
  This one is the crazy one. Suppose a world where we actually use the table schema for predicates, and update the table schema as we go along. Yes, it sucks when you add a new predicate (and the cell values are unclear) but for small worlds (or, "short schemas") it may (or may not) be interesting.


### New Iterators
#### Predicate Iterator
  Really, this is just the generalized value comparison iterator, across strings and dates and such.

## Longer Term (and fuzzy)

### SPARQL and more traditional RDF
  There's a whole body of work there, and a lot of interested researchers. They're the choir who already know the sermon of graph stores. Once ease-of-use gets people in the door, supporting extensions that make everyone happy seems like a win. And because we're query-language agnostic, it's a cleaner win. See also bootstrapping, which is the first goal toward this (eg, let's talk about sameAs, and index it appropriately.)

### Replication
  Technically it works now if you piggyback on someone else's replication, but that's cheating.  We speak HTTP, we can send quad sets over the wire to some other instance. Bonus points for a way to apply morphisms first -- massive graph on the backend, important graph on the frontend.

### Related services
  Eg, topic service, recon service -- whether in Cayley itself or as part of the greater project.

### New languages
  Javascript is nice for first-timers but experienced graph folks may want something more. Experiment with new languages, including but not limited to things that feel a lot like Datalog.

### Meta-stores
  Imagine an in-memory graph cache wrapped around another store.

### New Iterators
#### Cross Iterator
  This is the nutty one that's undefined and slow. The notion is to make graph-shapes more efficient, but it's unclear how that'll happen.


### All sorts of backends:
#### Git?
  Can we access git in a meaningful fashion, giving a history and rollbacks to memory/flat files?
#### ElasticSearch
#### Cassandra
#### Redis
