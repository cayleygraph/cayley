## UI Overview

### Sidebar

Along the side are the various actions or views you can take. From the top, these are:

- Run Query (run the query)
- Gizmo (a dropdown, to pick your query language, MQL is the other)
  - [GizmoAPI.md](GizmoAPI.md): This is the one of the two query languages used either via the REPL or HTTP interface.
  - [MQL.md](MQL.md): The _other_ query language the interfaces support.

---

- Query (a request/response editor for the query language)
- Query Shape (a visualization of the shape of the final query. Does not execute the query.)
- Visualize (runs a query and, if tagged correctly, gives a sigmajs view of the results)
- Write (an interface to write or remove individual quads or quad files)

---

- Documentation (this documentation)

### Visualize

To use the visualize function, emit, either through tags or JS post-processing, a set of JSON objects containing the keys `source` and `target`. These will be the links, and nodes will automatically be detected.

For example:

```javascript
[
  {
    source: "node1",
    target: "node2"
  },
  {
    source: "node1",
    target: "node3"
  }
];
```

Other keys are ignored. The upshot is that if you use the "Tag" functionality to add "source" and "target" tags, you can extract and quickly view subgraphs.

```
// Visualize who dani follows.
g.V("<dani>").Tag("source").Out("<follows>").Tag("target").All()
```

The visualizer expects to tag nodes as either "source" or "target." Your source is represented as a blue node.
While your target is represented as an orange node.
The idea being that our node relationship goes from blue to orange (source to target).

---
