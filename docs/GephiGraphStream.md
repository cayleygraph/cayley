# Gephi GraphStream

Cayley supports graph visualisation in Gephi using GraphStream API.

Enpoint can be accessed by adding URL `http://localhost:64210/gephi/gs` to Gephi GraphStream client.

## Options

### `limit`

Default: `10000`

Sets a maximal number of object that will be streamed.
Depending on stream mode this could be either nodes or quads.

Values less than 0 interpreted as "no limit".

### `mode`

Sets streaming mode. Supported values:

* `raw` (default) - all or selected quads
* `nodes` - nodes with properties

#### Raw mode

In this mode Cayley directly streams selected quads to Gephi.

Example URLs:

`/gephi/gs?mode=raw&pred=<follows>&limit=-1` (all quads)

`/gephi/gs?mode=raw&sub=<bob>&pred=<follows>,<status>&limit=-1` (links from `<bob>` via either `<follows>` or `<status>`)

Parameters:

* `limit` - maximal number of quads returned
* `sub`,`pred`,`obj`,`label` - only show quads with specified values of Subject, Predicate, Object or Label

This mode may be useful to visualize small subgraphs, or graphs without metadata such as types and properties.
In case of later, large number of quads will be pointing to nodes describing common types or property names.
For this kind of graphs `nodes` mode should be used.

#### Nodes with properties

Example URL: `/gephi/gs?mode=nodes&limit=-1`

In this mode Cayley streams all nodes and links associated with them, but instead of streaming common quads such as types
it will inline them as Gephi properties.
 
 Limit corresponds to a number of nodes returned.

By default, all predicate will be streamed as in `raw` mode, except well-known predicates and ones with `<gephi:inline> = "true"`.

List of well-known predicates includes:

* `<rdf:type>` (`<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>`)
* `<rdfs:label>` (`<http://www.w3.org/2000/01/rdf-schema#label>`)
* `<schema:name>` (`<http://schema.org/name>`)

To add custom predicates, write a special triple to database:

```nquads
<myCustomProperty> <gephi:inline> "true"^^<schema:Boolean> .
```

This allows to partition nodes by type or specific property values.

Note: Only one value per predicate is supported for inlined properties.

By default nodes will have random positions. To specify an exact position for specific node add `<gephi:x>` and `<gephi:y>` properties:

```nquads
<node> <gephi:x> "10"^^<schema:Integer>
<node> <gephi:y> "-12.3"^^<schema:Float>
```