# HTTP Methods

This file covers deprecated v1 HTTP API. All the methods of v2 HTTP API is described in OpenAPI/Swagger [spec](./api/swagger.yml)
and can be viewed by importing `https://raw.githubusercontent.com/cayleygraph/cayley/master/docs/api/swagger.yml` URL into [Swagger Editor](https://editor.swagger.io/) or [Swagger UI demo](http://petstore.swagger.io/).

## Gephi

Cayley supports streaming to Gephi via [GraphStream](GephiGraphStream.md).

## API v1

Unless otherwise noted, all URIs take a POST command.

### Queries and Results

#### `/api/v1/query/gizmo`

POST Body: Javascript source code of the query

Response: JSON results, depending on the query.

#### `/api/v1/query/graphql`

POST Body: [GraphQL](GraphQL.md) query

Response: JSON results, depending on the query.

#### `/api/v1/query/mql`

POST Body: JSON MQL query

Response: JSON results, with a query wrapper:
```js
{
	"result": <JSON Result set>
}
```

If the JSON is invalid or an error occurs:

```js
{
	"error": "Error message"
}
```


### Query Shapes

Result form:

```js
{
	"nodes": [{
		"id" : integer,
		"tags": ["list of tags from the query"],
		"values": ["known values from the query"],
		"is_link_node": bool,  // Does the node represent the link or the node (the oval shapes)
		"is_fixed": bool  // Is the node a fixed starting point of the query
	}],

	"links": [{
		"source": integer,  // Node ID
		"target": integer,  // Node ID
		"link_node": integer  // Node ID
	}]
}
```

#### `/api/v1/shape/gizmo`

POST Body: Javascript source code of the query

Response: JSON description of the last query executed.

#### `/api/v1/shape/mql`

POST Body: JSON MQL query

Response: JSON description of the query.

### Write commands

Responses come in the form

200 Success:
```js
{
	"result": "Success message."
}
```

400 / 500 Error:
```js
{
	"error": "Error message."
}
```

#### `/api/v1/write`

POST Body: JSON quads

```js
[{
	"subject": "Subject Node",
	"predicate": "Predicate Node",
	"object": "Object node",
	"label": "Label node"  // Optional
}]   // More than one quad allowed.
```

Response: JSON response message


#### `/api/v1/write/file/nquad`

POST Body: Form-encoded body:
 * Key: `NQuadFile`, Value: N-Quad file to write.

Response: JSON response message

Example:
```
curl http://localhost:64210/api/v1/write/file/nquad -F NQuadFile=@30k.n3
```

#### `/api/v1/delete`

POST Body: JSON quads

```js
[{
	"subject": "Subject Node",
	"predicate": "Predicate Node",
	"object": "Object node",
	"label": "Label node"  // Optional
}]   // More than one quad allowed.
```

Response: JSON response message.
