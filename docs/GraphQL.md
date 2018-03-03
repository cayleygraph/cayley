# GraphQL Guide

**Disclaimer:** Cayley's GraphQL implementation is not strictly a GraphQL, but only a query language with the same syntax and mostly the same rules.

We will use [this simple dataset](../data/testdata.nq) for our examples.

Every query is represented by tree-like structure of nested objects and properties, similar to [MQL](MQL.md).

```graphql
{
  nodes{
    id
  }
}
```

This particular query is equivalent to all nodes in the graph, where `id` is the special field name for the value of the node itself.

First root object in traditional GraphQL (named `nodes` here) represents a method that will be called on the server to get results.
In our current implementation this name serves only as a placeholder and will always execute the object search query.

Our example returns the following result:

```json
{
  "data": {
    "nodes": [
      {"id": "bob"},
      {"id": "status"},
      {"id": "cool_person"},
      {"id": "alice"},
      {"id": "greg"},
      {"id": "emily"},
      {"id": "smart_graph"},
      {"id": "predicates"},
      {"id": "dani"},
      {"id": "fred"},
      {"id": "smart_person"},
      {"id": "charlie"},
      {"id": "are"},
      {"id": "follows"}
    ]
  }
}
```

First level of JSON object corresponds to a request itself, thus either `data` field or `errors` will be present.

Any nested objects will correspond to fields defined in query, including top-level name (`nodes`).

### Limit and pagination

Maximal number of results can be limited using `first` keyword:

```graphql
{
  nodes(first: 10){
    id
  }
}
```

Pagination can be done with `offset` keyword:

```graphql
{
  nodes(offset: 5, first: 3){
    id
  }
}
```

This query returns objects 5-7.

*Note: Values might be sorted differently, depending on what backend is used.*

### Properties

Predicates (or properties) are added to the object to specify additional fields to load:

```graphql
{
  nodes{
    id, status
  }
}
```

Results:

```json
{
  "data": {
    "nodes": [
      {"id": "bob", "status": "cool_person"},
      {"id": "greg", "status": "cool_person"},
      {"id": "dani", "status": "cool_person"},
      {"id": "greg", "status": "smart_person"},
      {"id": "emily", "status": "smart_person"}
    ]
  }
}
```

All predicates are interpreted as IRIs and can be written in plain text or with angle brackets: `status` and `<status>` are considered equal.
Also, well-known namespaces like RDF, RDFS and Schema.org can be written in short form and will be expanded automatically: `schema:name` and `<schema:name>` will be expanded to `<http://schema.org/name>`.

Properties are required to be present by default and can be set to optional with `@opt` or `@optional` directive:

```graphql
{
  nodes{
    id
    status @opt
  }
}
```

Results:

```json
{
  "data": {
    "nodes": [
      {"id": "bob", "status": "cool_person"},
      {"id": "status"},
      {"id": "cool_person"},
      {"id": "alice"},
      {"id": "greg", "status": ["cool_person", "smart_person"]},
      {"id": "emily", "status": "smart_person"},
      {"id": "smart_graph"},
      {"id": "predicates"},
      {"id": "dani", "status": "cool_person"},
      {"id": "fred"},
      {"id": "smart_person"},
      {"id": "charlie"},
      {"id": "are"},
      {"id": "follows"}
    ]
  }
}
```
*Note: Since Cayley has no knowledge about property types and schema, it might decide to return a property as a single value for one object and as an array for another object. This behavior will be fixed in future versions.*

### Nested objects

Objects and properties can be nested:

```graphql
{
  nodes{
    id
    follows {
      id
    }
  }
}
```

All operations available on root also works for nested object, for example the limit:

```graphql
{
  nodes(first: 10){
    id
    follows(first: 1){
      id
    }
  }
}
```

### Reversed predicates

Any predicate can be reversed with `@rev` or `@reverse` directive (search for "in" links instead of "out"):

```graphql
{
  nodes{
    id
    followed: <follows> @rev {
      id
    }
  }
}
```

### Filters

Objects can be filtered by specific values of properties:

```graphql
{
  nodes(id: <bob>, status: "cool_person"){
    id
  }
}
```

Only exact match is supported for now.

GraphQL names are interpreted as IRIs and string literals are interpreted as strings.
Boolean, integer and float value are also supported and will be converted to `schema:Boolean`, `schema:Integer` and `schema:Float` accordingly.

### Labels

Any fields and traversals can be filtered by quad label with `@label` directive:

```graphql
{
  nodes{
    id
    follows @label(v: <fb>) {
      id, name
      follows @label {
        id, name
      }
    }
  }
}
```

Label will be inherited by child objects. To reset label filter add `@label` directive without parameters.

### Expanding all properties

To expand all properties of an object, `*` can be used instead of property name:

```graphql
{
  nodes{
    id
    follows {*}
  }
}
```

### Un-nest objects

The following query will return objects with `{id: x, status: {name: y}}` structure:

```graphql
{
  nodes{
    id
    status {
      name
    }
  }
}
```

It is possible to un-nest `status` field object into parent:

```graphql
{
  nodes{
    id
    status @unnest {
      status: name
    }
  }
}
```

Resulted objects will have a flat structure: `{id: x, status: y}`.

Arrays fields cannot be un-nested. You can still un-nest such fields by
providing a limit directive (will select the first value from array):

```graphql
{
  nodes{
    id
    statuses(first: 1) @unnest {
      status: name
    }
  }
}
```