/**
 * Both `.Morphism()` and `.Vertex()` create path objects, which provide the following traversal methods.
 * Note that `.Vertex()` returns a query object, which is a subclass of path object.
 *
 * For these examples, suppose we have the following graph:
 *
 * ```
 * +-------+                        +------+
 * | alice |-----                 ->| fred |<--
 * +-------+     \---->+-------+-/  +------+   \-+-------+
 *               ----->| #bob# |       |         |*emily*|
 * +---------+--/  --->+-------+       |         +-------+
 * | charlie |    /                    v
 * +---------+   /                  +--------+
 *   \---    +--------+             |*#greg#*|
 *       \-->| #dani# |------------>+--------+
 *           +--------+
 * ```
 *
 * Where every link is a `<follows>` relationship, and the nodes with an extra `#` in the name have an extra `<status>` link. As in,
 *
 * ```
 * <dani> -- <status> --> "cool_person"
 * ```
 *
 * Perhaps these are the influencers in our community. So too are extra `*`s in the name -- these are our smart people,
 * according to the `<smart_graph>` label, eg, the quad:
 *
 * ```
 * <greg> <status> "smart_person" <smart_graph> .
 * ```
 */
interface Path {
  /** Execute the query and adds the results, with all tags, as a string-to-string (tag to node) map in the output set, one for each path that a traversal could take. */
  all(): void;
  /** Alias for intersect. */
  and(path: Path): Path;
  /** Alias for tag. */
  as(...tags: string[]): Path;
  /** Return current path to a set of nodes on a given tag, preserving all constraints.
   * If still valid, a path will now consider their vertex to be the same one as the previously tagged one, with the added constraint that it was valid all the way here. Useful for traversing back in queries and taking another route for things that have matched so far. */
  back(tag?: string): Path;
  /** Follow the predicate in either direction. Same as out or in. */
  both(path: Path, ...tags: string[]): Path;
  /** Return a number of results and returns it as a value. */
  count(): number;
  /** Alias for Except */
  difference(path: Path): Path;
  /** Removes all paths which match query from current path. In a set-theoretic sense, this is (A - B). While `g.V().Except(path)` to achieve `U - B = !B` is supported, it's often very slow. */
  except(path: Path): Path;
  /** Apply constraints to a set of nodes. Can be used to filter values by range or match strings. */
  filter(...args: any): Path;
  /** The way to use a path prepared with Morphism. Applies the path chain on the morphism object to the current path.
   * Starts as if at the g.M() and follows through the morphism path. */
  follow(path: Path): Path;
  /** The same as follow but follows the chain in the reverse direction. Flips "In" and "Out" where appropriate,
the net result being a virtual predicate followed in the reverse direction. Starts at the end of the morphism and follows it backwards (with appropriate flipped directions) to the g.M() location. */
  followR(path: Path): Path;
  /** The same as follow but follows the chain recursively. Starts as if at the g.M() and follows through the morphism path multiple times, returning all nodes encountered. */
  followRecursive(path: Path): Path;
  /** Call callback(data) for each result, where data is the tag-to-string map as in All case.
   * @param [limit] An integer value on the first `limit` paths to process.
   * @param callback: A javascript function of the form `function(data)`
   */
  forEach(callback: (data: { [key: string]: any }) => void): void;
  forEach(
    limit: number,
    callback: (data: { [key: string]: any }) => void
  ): void;
  /** Alias for forEach. */
  map(callback: (data: { [key: string]: any }) => void): void;
  map(limit: number, callback: (data: { [key: string]: any }) => void): void;
  /** The same as All, but limited to the first N unique nodes at the end of the path, and each of their possible traversals. */
  getLimit(limit: number): void;
  /** Filter all paths which are, at this point, on the subject for the given predicate and object,
but do not follow the path, merely filter the possible paths. Usually useful for starting with all nodes, or limiting to a subset depending on some predicate/value pair.
  * @param predicate A string for a predicate node.
  * @param object A string for a object node or a set of filters to find it.
  */
  has(predicate: string, object: string): Path;
  /** The same as Has, but sets constraint in reverse direction. */
  hasR(predicate: string, object: string): Path;
  /** The inverse of out. Starting with the nodes in `path` on the object, follow the quads with predicates defined by `predicatePath` to their subjects.
   * @param [predicatePath] One of:
   * * null or undefined: All predicates pointing into this node
   * * a string: The predicate name to follow into this node
   * * a list of strings: The predicates to follow into this node
   * * a query path object: The target of which is a set of predicates to follow.
   * @param [tags] One of:
   * * null or undefined: No tags
   * * a string: A single tag to add the predicate used to the output set.
   * * a list of strings: Multiple tags to use as keys to save the predicate used to the output set.
   */
  in(predicatePath?: Path, ...tags: string[]): Path;
  /** Get the list of predicates that are pointing in to a node. */
  inPredicates(): Path;
  /** Filter all paths by the result of another query path. This is essentially a join where, at the stage of each path, a node is shared. */
  intersect(path: Path): Path;
  /** Filter all paths to ones which, at this point, are on the given node.
   * @param node: A string for a node. Can be repeated or a list of strings.
   */
  is(node: string, ...nodes: string[]): Path;
  /** Set (or remove) the subgraph context to consider in the following traversals.
   * Affects all in(), out(), and both() calls that follow it. The default LabelContext is null (all subgraphs).
   * @param predicatePath One of:
   * * null or undefined: In future traversals, consider all edges, regardless of subgraph.
   * * a string: The name of the subgraph to restrict traversals to.
   * * a list of strings: A set of subgraphs to restrict traversals to.
   * * a query path object: The target of which is a set of subgraphs.
   * @param tags One of:
   * * null or undefined: No tags
   * * a string: A single tag to add the last traversed label to the output set.
   * * a list of strings: Multiple tags to use as keys to save the label used to the output set.
   */
  labelContext(labelPath: Path, ...tags: string[]): Path;
  /** Get the list of inbound and outbound quad labels */
  labels(): Path;
  /** Limit a number of nodes for current path. */
  limit(limit: number): Path;
  /** Alias for Union. */
  or(path: Path): Path;
  /** The work-a-day way to get between nodes, in the forward direction. Starting with the nodes in `path` on the subject, follow the quads with predicates defined by `predicatePath` to their objects.
   * @param predicatePath (Optional): One of:
   * * null or undefined: All predicates pointing out from this node
   * * a string: The predicate name to follow out from this node
   * * a list of strings: The predicates to follow out from this node
   * * a query path object: The target of which is a set of predicates to follow.
   * @param tags (Optional): One of:
   * * null or undefined: No tags
   * * a string: A single tag to add the predicate used to the output set.
   * * a list of strings: Multiple tags to use as keys to save the predicate used to the output set.
   */
  out(predicatePath?: Path, ...tags: string[]): Path;
  /** Get the list of predicates that are pointing out from a node. */
  outPredicates(): Path;
  /** Save the object of all quads with predicate into tag, without traversal.
   * @param predicate A string for a predicate node.
   * @param tag A string for a tag key to store the object node.
   */
  save(predicate: string, tag: string): Path;
  /** The same as save, but returns empty tags if predicate does not exists. */
  saveOpt(predicate: string, tag: string): Path;
  /** The same as saveOpt, but tags values via reverse predicate. */
  saveOptR(predicate: string, tag: string): Path;
  /** The same as save, but tags values via reverse predicate. */
  saveR(predicate: string, tag: string): Path;
  /** Tag the list of predicates that are pointing in to a node. */
  saveInPredicates(tag: string): Path;
  /** Tag the list of predicates that are pointing out from a node. */
  saveOutPredicates(tag: string): Path;
  /** Skip a number of nodes for current path.
   * @param offset: A number of nodes to skip.
   */
  skip(offset: number): Path;
  /** Save a list of nodes to a given tag. In order to save your work or learn more about how a path got to the end, we have tags.
The simplest thing to do is to add a tag anywhere you'd like to put each node in the result set.
   * @param tag: A string or list of strings to act as a result key. The value for tag was the vertex the path was on at the time it reached "Tag" */
  tag(...tags: string[]): Path;
  /**
   * The same as toArray, but instead of a list of top-level nodes, returns an Array of tag-to-string dictionaries, much as All would, except inside the JS environment.
   */
  tagArray(): void;
  /** The same as TagArray, but limited to one result node. Returns a tag-to-string map. */
  tagValue(): void;
  /** Execute a query and returns the results at the end of the query path as an JS array. */
  toArray(): void;
  /** The same as ToArray, but limited to one result node. */
  toValue(): void;
  /** Return the combined paths of the two queries. Notice that it's per-path, not per-node. Once again, if multiple paths reach the same destination, they might have had different ways of getting there (and different tags). See also: `Path.prototype.tag()` */
  union(path: Path): Path;
  /** Remove duplicate values from the path. */
  unique(): Path;
}

interface Graph {
  /** A shorthand for Vertex. */
  V(...nodeId: string[]): Path;
  /** A shorthand for Morphism */
  M(): Path;
  /** Start a query path at the given vertex/vertices. No ids means "all vertices". */
  Vertex(...nodeId: string[]): Path;
  /** Create a morphism path object. Unqueryable on it's own, defines one end of the path.
Saving these to variables with */
  Morphism(): Path;
  /** Load all namespaces saved to graph. */
  loadNamespaces(): void;
  /** Register all default namespaces for automatic IRI resolution. */
  addDefaultNamespaces(): void;
  /** Associate prefix with a given IRI namespace. */
  addNamespace(): void;
  /** Add data programmatically to the JSON result list. Can be any JSON type. */
  emit(): void;
  /** Create an IRI values from a given string. */
  IRI(): string;
}

/** This is the only special object in the environment, generates the query objects.
Under the hood, they're simple objects that get compiled to a Go iterator tree when executed. */
declare var graph: Graph;

/** Alias of graph. This is the only special object in the environment, generates the query objects.
Under the hood, they're simple objects that get compiled to a Go iterator tree when executed. */
declare var g: Graph;

interface RegexFilter {}

/** Filter by match a regular expression ([syntax](https://github.com/google/re2/wiki/Syntax)). By default works only on literals unless includeIRIs is set to `true`. */
declare function regex(expression: string, includeIRIs?: boolean): RegexFilter;
