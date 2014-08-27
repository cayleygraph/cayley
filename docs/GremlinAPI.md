# Javascript/Gremlin API documentation

## The `graph` object

Alias: `g`

This is the only special object in the environment, generates the query objects. Under the hood, they're simple objects that get compiled to a Go iterator tree when executed.

#### **`graph.Vertex([nodeId],[nodeId]...)`**

Alias: `graph.V`

Arguments:

  * `nodeId` (Optional): A string or list of strings representing the starting vertices.

Returns: Query object

Starts a query path at the given vertex/verticies. No ids means "all vertices".

####**`graph.Morphism()`**

Alias: `graph.M`

Arguments: none

Returns: Path object

Creates a morphism path object. Unqueryable on it's own, defines one end of the path. Saving these to variables with

```javascript
var shorterPath = graph.Morphism().Out("foo").Out("bar")
```

is the common use case. See also: `path.Follow()`, `path.FollowR()`

####**`graph.Emit(data)`**

Arguments:

  * `data`: A Javascript object that can be serialized to JSON

Adds data programatically to the JSON result list. Can be any JSON type.


## Path objects

Both `.Morphism()` and `.Vertex()` create path objects, which provide the following traversal methods.

For these examples, suppose we have the following graph:
```
  +---+                        +---+
  | A |-------               ->| F |<--
  +---+       \------>+---+-/  +---+   \--+---+
               ------>|#B#|      |        | E |
  +---+-------/      >+---+      |        +---+
  | C |             /            v
  +---+           -/           +---+
    ----    +---+/             |#G#|
        \-->|#D#|------------->+---+
            +---+
```

Where every link is a "follows" relationship, and the nodes with an extra `#` in the name have an extra `status` link. As in,

```
D -- status --> cool_person
```

Perhaps these are the influencers in our community.

### Basic Traversals

####**`path.Out([predicatePath], [tags])`**

Arguments:

  * `predicatePath` (Optional): One of:
    * null or undefined: All predicates pointing out from this node
    * a string: The predicate name to follow out from this node
	* a list of strings: The predicates to follow out from this node
	* a query path object: The target of which is a set of predicates to follow.
  * `tags` (Optional): One of:
	* null or undefined: No tags
	* a string: A single tag to add the predicate used to the output set.
	* a list of strings: Multiple tags to use as keys to save the predicate used to the output set.

Out is the work-a-day way to get between nodes, in the forward direction. Starting with the nodes in `path` on the subject, follow the quads with predicates defined by `predicatePath` to their objects.

Example:
```javascript
// The working set of this is B and D
g.V("C").Out("follows")
// The working set of this is F, as A follows B and B follows F.
g.V("A").Out("follows").Out("follows")
// Finds all things D points at. Result is B G and cool_person
g.V("D").Out()
// Finds all things D points at on the status linkage.
// Result is B G and cool_person
g.V("D").Out(["follows", "status"])
// Finds all things D points at on the status linkage, given from a seperate query path.
// Result is {"id": cool_person, "pred": "status"}
g.V("D").Out(g.V("status"), "pred")
```

####**`path.In([predicatePath], [tags])`**

Arguments:

  * `predicatePath` (Optional): One of:
	* null or undefined: All predicates pointing into this node
	* a string: The predicate name to follow into this node
	* a list of strings: The predicates to follow into this node
	* a query path object: The target of which is a set of predicates to follow.
  * `tags` (Optional): One of:
	* null or undefined: No tags
	* a string: A single tag to add the predicate used to the output set.
	* a list of strings: Multiple tags to use as keys to save the predicate used to the output set.

Same as Out, but in the other direction.  Starting with the nodes in `path` on the object, follow the quads with predicates defined by `predicatePath` to their subjects.

Example:
```javascript
// Find the cool people, B G and D
g.V("cool_person").In("status")
// Find who follows B, in this case, A, C, and D
g.V("B").In("follows")
// Find who follows the people E follows, namely, E and B
g.V("E").Out("follows").In("follows")
```

####**`path.Both([predicatePath], [tags])`**

Arguments:

  * `predicatePath` (Optional): One of:
	* null or undefined: All predicates pointing both into and out from this node
	* a string: The predicate name to follow both into and out from this node
	* a list of strings: The predicates to follow both into and out from this node
	* a query path object: The target of which is a set of predicates to follow.
  * `tags` (Optional): One of:
	* null or undefined: No tags
	* a string: A single tag to add the predicate used to the output set.
	* a list of strings: Multiple tags to use as keys to save the predicate used to the output set.
Follow the predicate in either direction. Same as

Note: Less efficient, for the moment, as it's implemented with an Or, but useful where necessary.

Example:
```javascript
// Find all followers/followees of F. Returns B E and G
g.V("F").Both("follows")
```


####**`path.Is(node, [node..])`**

Arguments:

  * `node`: A string for a node. Can be repeated or a list of strings.

Filter all paths to ones which, at this point, are on the given `node`.

Example:
```javascript
// Starting from all nodes in the graph, find the paths that follow B.
// Results in three paths for B (from A C and D)
g.V().Out("follows").Is("B")
```

####**`path.Has(predicate, object)`**

Arguments:

  * `predicate`: A string for a predicate node.
  * `object`: A string for a object node.

Filter all paths which are, at this point, on the subject for the given predicate and object, but do not follow the path, merely filter the possible paths.

Usually useful for starting with all nodes, or limiting to a subset depending on some predicate/value pair.

Example:
```javascript
// Start from all nodes that follow B -- results in A C and D
g.V().Has("follows", "B")
// People C follows who then follow F. Results in B.
g.V("C").Out("follows").Has("follows", "F")
```

### Tagging

####**`path.Tag(tag)`**

Alias: `path.As`

Arguments:

  * `tag`: A string or list of strings to act as a result key. The value for tag was the vertex the path was on at the time it reached "Tag"

In order to save your work or learn more about how a path got to the end, we have tags. The simplest thing to do is to add a tag anywhere you'd like to put each node in the result set.


Example:
```javascript
// Start from all nodes, save them into start, follow any status links, and return the result.
// Results are: {"id": "cool_person", "start": "B"}, {"id": "cool_person", "start": "G"}, {"id": "cool_person", "start": "D"}
g.V().Tag("start").Out("status")
```


####**`path.Back(tag)`**

Arguments:

   * `tag`: A previous tag in the query to jump back to.

If still valid, a path will now consider their vertex to be the same one as the previously tagged one, with the added constraint that it was valid all the way here. Useful for traversing back in queries and taking another route for things that have matched so far.

Example:
```javascript
// Start from all nodes, save them into start, follow any status links, jump back to the starting node, and find who follows them. Return the result.
// Results are:
//   {"id": "A", "start": "B"},
//   {"id": "C", "start": "B"},
//   {"id": "D", "start": "B"},
//   {"id": "C", "start": "D"},
//   {"id": "D", "start": "G"}
g.V().Tag("start").Out("status").Back("start").In("follows")
```

####**`path.Save(predicate, tag)`**

Arguments:

  * `predicate`: A string for a predicate node.
  * `tag`: A string for a tag key to store the object node.

From the current node as the subject, save the object of all quads with `predicate` into `tag`, without traversal.

Example:
```javascript
// Start from D and B and save who they follow into "target"
// Returns:
//   {"id" : "D", "target": "B" },
//   {"id" : "D", "target": "G" },
//   {"id" : "B", "target": "F" },
g.V("D", "B").Save("follows", "target")
```

### Joining

####**`path.Intersect(query)`**

Alias: `path.And`

Arguments:

  * `query`: Antother query path, the result sets of which will be intersected

Filters all paths by the result of another query path (efficiently computed).

This is essentially a join where, at the stage of each path, a node is shared.
Example:
```javascript
var cFollows = g.V("C").Out("follows")
var dFollows = g.V("D").Out("follows")
// People followed by both C (B and D) and D (B and G) -- returns B.
cFollows.Intersect(dFollows)
// Equivalently, g.V("C").Out("follows").And(g.V("D").Out("follows"))
```

####**`path.Union(query)`**

Alias: `path.Or`

Arguments:

  * `query`: Antother query path, the result sets of which will form a union

Given two queries, returns the combined paths of the two queries.
Notice that it's per-path, not per-node. Once again, if multiple paths reach the
same destination, they might have had different ways of getting there (and different tags).
See also: `path.Tag()`

Example:
```javascript
var cFollows = g.V("C").Out("follows")
var dFollows = g.V("D").Out("follows")
// People followed by both C (B and D) and D (B and G) -- returns B (from C), B (from D), D and G.
cFollows.Union(dFollows)
```

### Using Morphisms

####**`path.Follow(morphism)`**

Arguments:

  * `morphism`: A morphism path to follow

With `graph.Morphism` we can prepare a path for later reuse. `Follow` is the way that's accomplished.
Applies the path chain on the morphism object to the current path.

Starts as if at the g.M() and follows through the morphism path.

Example:
```javascript:
friendOfFriend = g.Morphism().Out("follows").Out("follows")
// Returns the followed people of who C follows -- a simplistic "friend of my frind"
// and whether or not they have a "cool" status. Potential for recommending followers abounds.
// Returns B and G
g.V("C").Follow(friendOfFriend).Has("status", "cool_person")
```

####**`path.FollowR(morphism)`**

Arguments:

  * `morphism`: A morphism path to follow

Same as `Follow` but follows the chain in the reverse direction. Flips "In" and "Out" where appropriate,
the net result being a virtual predicate followed in the reverse direction.

Starts at the end of the morphism and follows it backwards (with appropriate flipped directions) to the g.M() location.

Example:
```javascript:
friendOfFriend = g.Morphism().Out("follows").Out("follows")
// Returns the third-tier of influencers -- people who follow people who follow the cool people.
// Returns E B C (from B) and C (from G)
g.V().Has("status", "cool_person").FollowR(friendOfFriend)
```


## Query objects (finals)

Only `.Vertex()` objects -- that is, queries that have somewhere to start -- can be turned into queries. To actually execute the queries, an output step must be applied.

####**`query.All()`**

Arguments: None

Returns: undefined

Executes the query and adds the results, with all tags, as a string-to-string (tag to node) map in the output set, one for each path that a traversal could take.

####**`query.GetLimit(size)`**

Arguments:

  * `size`: An integer value on the first `size` paths to return.

 Returns: undefined

Same as all, but limited to the first `size` unique nodes at the end of the path, and each of their possible traversals.

####**`query.ToArray()`**

Arguments: None

Returns: Array

Executes a query and returns the results at the end of the query path.
Example:
``javascript
// fooNames contains an Array of names for foo.
var fooNames = g.V("foo").Out("name").ToArray()
``

####**`query.ToValue()`**

Arguments: None

Returns: String

As `.ToArray` above, but limited to one result node -- a string. Like `.Limit(1)` for the above case.

####**`query.TagArray()`**

Arguments: None

Returns: Array of string-to-string objects

As `.ToArray` above, but instead of a list of top-level strings, returns an Array of tag-to-string dictionaries, much as `.All` would, except inside the Javascript environment.
Example:
``javascript
// fooNames contains an Array of names for foo.
var fooTags = g.V("foo").Tag("foo_value").Out("name").ToArray()
// fooValue should be the string "foo"
var fooValue = fooTags[0]["foo_value"]
``

####**`query.TagValue()`**

Arguments: None

Returns: A single string-to-string object

As `.TagArray` above, but limited to one result node -- a string. Like `.Limit(1)` for the above case. Returns a tag-to-string map.


####**`query.ForEach(callback), query.ForEach(limit, callback)`**

Alias: `query.Map`

Arguments:

  * `limit` (Optional): An integer value on the first `limit` paths to process.
  * `callback`: A javascript function of the form `function(data)`

Returns: undefined

For each tag-to-string result retrieved, as in the `All` case, calls `callback(data)` where `data` is the tag-to-string map.

Example:
```javascript
// Simulate query.All()
graph.V("foo").ForEach(function(d) { g.Emit(d) } )
```
