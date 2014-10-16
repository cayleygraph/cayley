# MQL Guide

## General

Cayley's MQL implementation is a work-in-progress clone of [Freebase's MQL API](https://developers.google.com/freebase/mql/). At the moment, it supports very basic queries without some of the extended features. It also aims to be database-agnostic, meaning that the schema inference from Freebase does not (yet) apply.

Every JSON Object can be thought of as a node in the graph, and wrapping an object in a list means there may be several of these, or it may be repeated. A simple query like:

```json
[{
  "id": null
}]
```

Is equivalent to all nodes in the graph, where "id" is the special keyword for the value of the node.

Predicates are added to the object to specify constraints.

```json
[{
  "id": null,
  "some_predicate": "some value"
}]
```

Predicates can take as values objects or lists of objects (subqueries), strings and numbers (literal IDs that must match -- equivalent to the object {"id": "value"}) or null, which indicates that, while the object must have a predicate that matches, the matching values will replace the null. A single null is one such value, an empty list will be filled with all such values, as strings.

## Keywords

* `id`: The value of the node.

## Reverse Predicates

Predicates always assume a forward direction. That is,

```json
[{
  "id": "A",
  "some_predicate": "B"
}]
```

will only match if the quad
```
A some_predicate B .
```

exists. In order to reverse the directions, "!predicates" are used. So that:

```json
[{
  "id": "A",
  "!some_predicate": "B"
}]
```

will only match if the quad
```
B some_predicate A .
```
exists.

## Multiple Predicates

JSON does not specify the behavior of objects with the same key. In order to have separate constraints for the same predicate, the prefix "@name:" can be applied to any predicate. This is slightly different from traditional MQL in that fully-qualified http paths may be common predicates, so we have an "@name:" prefix instead.


```json
[{
  "id": "A",
  "@x:some_predicate": "B",
  "@y:some_predicate": "C"

}]
```

Will only match if *both*

```
A some_predicate B .
A some_predicate C .
```

exist.

This combines with the reversal rule to create paths like ``"@a:!some_predicate"``

## Union Queries (aka or-equals)

To query for predicates that match one of a number of constraints the suffix `"|="` can be used along with an array of subqueries.  Subqueries can be simple strings to match against specific values or they can also be objects that describe constraints to match against.

```json
[{
  "id|=": ["A", "B"]
}]
```

Will match *either* A or B.

```json
[{
  "id|=": [
    {
      "follows": "F"
    },
    {
      "status": "cool"
    }
  ]
}]
```

Will match everyone who follows F or is cool.
