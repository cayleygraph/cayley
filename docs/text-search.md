# Text Search

### Overview

Cayley supports query operations that perform a text search of string content. To perform text search, Cayley uses a text index and the `/search` endpoint.

### Example

This example demonstrates how to build a text index and use it to find coffee shops, given only text fields.

Load the following documents:

```json
[
  {
    "@id": "java-hut",
    "@type": "CoffeeShop",
    "name": "Java Hut",
    "description": "Coffee and cakes"
  },
  {
    "@id": "burger-bugs",
    "@type": "CoffeeShop",
    "name": "Burger Buns",
    "description": "Gourmet hamburgers"
  },
  {
    "@id": "coffee-shop",
    "@type": "CoffeeShop",
    "name": "Coffee Shop",
    "description": "Just coffee"
  },
  {
    "@id": "clothes-clothes-clothes",
    "@type": "CoffeeShop",
    "name": "Clothes Clothes Clothes",
    "description": "Discount clothing"
  },
  {
    "@id": "java-shopping",
    "@type": "CoffeeShop",
    "name": "Java Shopping",
    "description": "Indonesian goods"
  }
]
```

#### Text Index

Cayley provides text indexes to support text search queries on string content.

To perform text search queries, you must have a text index on your data-set.

Add the following configuration to `cayley.yaml`:

```yaml
searchIndexes:
  - name: coffee-shops
    match:
      - "@type": CoffeeShop
    properties:
      - name: name
        type: text
      - name: description
        type: text
```

#### `/search` endpoint

Use the `/search` endpoint to perform text searches on a data-set with a text-index.

For example, you could use the following query to find all stores which `name` or `description` containing `"coffee"`:

```http
GET /search?q=coffee
```

You will get:

```json
[
  {
    "@id": "java-hut"
  },
  {
    "@id": "coffee-shop"
  }
]
```
