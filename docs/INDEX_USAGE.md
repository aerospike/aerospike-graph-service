## Vertex Property Index Usage
To create a vertex property index, use the following syntax in the aerospike-graph.properties file:
```
aerospike.graph.index.vertex.properties=property_key1,property_key2,...
```
Vertex property indexes are taken as a union from all firefly instances.
This means that if one firefly instance has:
```
aerospike.graph.index.vertex.properties=foo
```
And another has:
```
aerospike.graph.index.vertex.properties=bar
```
Then the vertex property index will be created for both `foo` and `bar`. Furthermore,
Firefly will enumerate all indexes periodically, so if one firefly instance creates
a new index `baz`, all other firefly instances will detect it and be able to use it.

When a vertex property index is first created on a dataset, the time it takes to create
the index is proportion to the amount of data in aerospike. This means it is best to create
the index before loading data, however it is possible to create the index after data is loaded.
The indexes can be set in the bulk loader before bulk loading as well.

## Vertex Label Index Usage

Vertex label indexes can be created by setting the flags:
```
aerospike.graph.index.vertex.properties=true
```
This flag is taken as a union from all firefly instances. This means if a single firefly instance sets the flag to true, 
all other firefly instances will be aware of the vertex label index that was created and will be able to use it.

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

#### Impact on Traversals

A vertex property index only affects the very start of a traversal and has no impact otherwise.
Considering the schema above, let's look at a few traversals.

A good vertex property to create an index on is one that is used to discriminate the dataset down to
one or very few vertices which the traversal can start from (some sort of highly unique identifier).

#### Some example traversals and the impact:

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
        _________________________ Firefly can compound has steps together at the start of a traversal, and even
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
        _________________________ In this case where both steps at the start of the traversal have indexes, firefly
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
        _________________________ In this case, firefly will note that both the label index exists and so does the 
       |                     |    "name" index. It will use the "name" index first because properties tend to have 
       |                     |    higher cardinality than labels.
       |                     |
       |                     |                      __________ None of these steps are impacted by the index
       |                     |                     |     |     because they are not at the start of a traversal
       v                     v                     V     v
g.V().hasLabel("Person").has("name", "Alice").out().has("name", "Bob").toList()
```

Label index and non-indexed vertex property:
```
        _________________________ In this case firefly will use the label index first because the property is not indexed. 
       |                     |    Note: A traversal like this approaches OLAP territory due to the way the constraints are
       |                     |          and are discouraged as a pattern.
       |                     |
       |                     |                      __________ None of these steps are impacted by the index
       |                     |                     |     |     because they are not at the start of a traversal
       v                     v                     V     v
g.V().hasLabel("Person").has("country", "USA").out().has("name", "Bob").toList()
```