Aerospike Graph provides an API to retrieve approximate 
metadata on the graph without having to scan the entire
graph.

The approximate metadata is stored in a virtual vertex
that can be retrieved as:
```
Vertex v = g.V("~firefly_approximate_statistics_vertex").next();
```
This vertex contains a series of properties that are detailed in the table below:

| Property Key                | Data Type                  | Notes                                                       |
|-----------------------------|----------------------------|-------------------------------------------------------------|
| vertex_count                | `long`                     | Total vertex count.                                         |
| edge_count                  | `long`                     | Total edge count.                                           |
| vertex_count_per_label      | `Map<String, Long>`        | Total vertex count for a given vertex label.                |
| edge_count_per_label        | `Map<String, Long>`        | Total edge count for a given edge label.                    |
| vertex_properties_per_label | `Map<String, Set<String>>` | Union of vertex property keys for a given vertex label.     |
| edge_properties_per_label   | `Map<String, Set<String>>` | Union of edge property keys for a given edge label.         |

Vertex and edge property keys are taken as a union, and therefore 
are persistent and will not be removed if the property is 
removed from all vertices or edges. The only way to the property 
metadata is to do a full `g.V().drop().iterate();`.

The metadata runs asynchronously and is meant to provide approximate 
statistics very quickly without having to scan the entire graph. Because
of this nature, the data in the metadata may lag behind the actual graph, and
may also experience skew if the graph write load on any given node reaches
extreme values.