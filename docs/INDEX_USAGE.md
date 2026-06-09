# Vertex and label index usage

Index design and tradeoffs: [`INDEX_DESIGN.md`](INDEX_DESIGN.md). Official
indexing guidance: [Indexing](https://aerospike.com/docs/graph/develop/query/indexing).

## Vertex property index usage

To create a vertex property index, add the following syntax to the
`aerospike-graph.properties` file:

```
aerospike.graph.index.vertex.properties=property_key1,property_key2,...
```

Vertex property indexes are taken as a union across all AGS instances.
If one instance has:

```
aerospike.graph.index.vertex.properties=foo
```

And another has:

```
aerospike.graph.index.vertex.properties=bar
```

Then the vertex property index is created for both `foo` and `bar`.
Aerospike Graph Service enumerates indexes periodically, so if one instance
creates a new index `baz`, other instances detect it on the next metadata
refresh and can use it.

When a vertex property index is first created on a dataset, creation time is
proportional to the amount of data in Aerospike. Create indexes before
loading data when you can. You can also create them after load or set them
in the bulk loader before bulk loading.

## Vertex label index usage

The vertex label index is opt-in. Enable it with:
```
aerospike.graph.index.vertex.label.enabled=true
```
This flag is taken as a union across all AGS instances. A single instance
setting it to `true` is enough. All other instances pick up the index on
their next metadata refresh and can use it.

## Edge indexes

Edge property and edge label indexes are **not** supported today.
Setting `aerospike.graph.index.edge.properties` or
`aerospike.graph.index.edge.label.enabled=true` will cause the service
to fail startup. See [`INDEX_DESIGN.md`](INDEX_DESIGN.md) for details.

## Example
For a graph with schema:
```
VERTICES:
label: "Person"
{ 
    "name": "John Doe",
    "age": 30,
    "address": "123 Main St",
    "city": "San Francisco",
    "state": "CA",
    "country": "USA",
    "zip": "94105"
}

EDGES:
label: "knows"
{
}
```
A sindex on name and age can be created by adding the following line to the aerospike-graph.properties file:
```
aerospike.graph.index.vertex.properties=name,age
aerospike.graph.index.vertex.label.enabled=true
```

### Impact on traversals

A vertex property index only affects the very start of a traversal and has no impact otherwise.
Considering the schema above, let's look at a few traversals.

A good vertex property to create an index on is one that is used to discriminate the dataset down to
one or very few vertices which the traversal can start from (some sort of highly unique identifier).

#### Some example traversals and the impact

Consider the example schema and indexes above with the below examples.

Single indexed vertex property:
```
        ______ This has step is affected by the index and instead of a full scan, it will use the index
       |       to evaluate this step very fast.
       |
       |                       _______________ None of these steps are impacted by the index
       |                      |    |     |     because they are not at the start of a traversal
       v                      V    v     v
g.V().has("name", "Alice").out().in().has("name", "Bob").toList()
```

Single non-indexed vertex property:
```
        ______ This has step is NOT affected by the index and will scan the entire database for this property.
       |       This is a very slow traversal to run and is discouraged.
       |  
       |                      __________ None of these steps are impacted by the index
       |                     |     |     because they are not at the start of a traversal
       v                     V     v
g.V().has("country", "USA").out().has("name", "Alice").toList()
```

One vertex property index and one non-indexed vertex property:
```
        _________________________ AGS can compound has steps together at the start of a traversal, and even
       |                     |    though the has("name", "Alice") is not at the start of the traversal, it will
       |                     |    be reordered to run first as an index and have the has("country", "USA") step
       |                     |    run after it, giving index performance.
       |                     |    
       |                     |                      __________ None of these steps are impacted by the index
       |                     |                     |     |     because they are not at the start of a traversal
       v                     v                     V     v
g.V().has("country", "USA").has("name", "Alice").out().has("name", "Bob").toList()
```

Two vertex property indexes:
```
        _________________________ When both steps at the start of the traversal have indexes, AGS
       |                     |    will use cardinality metadata that it periodically collects from Aerospike to 
       |                     |    determine which index to run first and which to apply as a filter after the index
       |                     |    Note: Cardinality metadata in Aerospike is only updated once an hour, so this will
       |                     |          long running systems much more than short test systems.
       |                     |
       |                     |                      __________ None of these steps are impacted by the index
       |                     |                     |     |     because they are not at the start of a traversal
       v                     v                     V     v
g.V().has("age", 29).has("name", "Alice").out().has("name", "Bob").toList()
```

Label index and vertex property index:
```
        _________________________ AGS notes that both the label index and the "name" index exist.
       |                     |    It uses the "name" index first because properties tend to have
       |                     |    higher cardinality than labels.
       |                     |
       |                     |                      __________ None of these steps are impacted by the index
       |                     |                     |     |     because they are not at the start of a traversal
       v                     v                     V     v
g.V().hasLabel("Person").has("name", "Alice").out().has("name", "Bob").toList()
```

Label index and non-indexed vertex property:
```
        _________________________ AGS uses the label index first because the property is not indexed.
       |                     |    Note: A traversal like this approaches OLAP territory due to the way the constraints are
       |                     |          and are discouraged as a pattern.
       |                     |
       |                     |                      __________ None of these steps are impacted by the index
       |                     |                     |     |     because they are not at the start of a traversal
       v                     v                     V     v
g.V().hasLabel("Person").has("country", "USA").out().has("name", "Bob").toList()
```