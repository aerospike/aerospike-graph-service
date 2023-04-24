Aerospike Graph provides an API to retrieve graph summary information 
without having to scan the entire graph.

The summary vertex is stored in a virtual vertex
that can be retrieved as:
```
Vertex v = g.V("~graph_summary").next();
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

The summary task runs asynchronously and is meant to provide approximate 
statistics very quickly without having to scan the entire graph. Because
of this nature, the data in the metadata may lag behind the actual graph, and
may also experience skew if the graph write load on any given node reaches
extreme values.

Below is a complete sample of using the summary vertex:
```
        // Add vertices and edges.
        final Vertex lyndon = g.addV("person").property("name", "Lyndon Bauto").property("age", 30).next();
        final Vertex simon = g.addV("person").property("name", "Simon Zhao").property("location", "Vancouver").next();
        final Vertex grant = g.addV("person").property("name", "Grant Haywood").next();
        final Vertex mycroft = g.addV("cat").property("breed", "Maine Coone").next();
        g.addE("knows").from(lyndon).to(simon).property("weight", 1.0).iterate();
        g.addE("knows").from(lyndon).to(grant).property("since", "2022").iterate();
        g.addE("knows").from(simon).to(grant).iterate();
        g.addE("swonk").from(grant).to(lyndon).iterate();
        g.addE("owns").from(lyndon).to(mycroft).iterate();
        
        // Add a sleep to allow the summary task to run.
        Thread.sleep(1000);
        
        // Get summary vertex.
        final Vertex summary = g.V("~graph_summary").next();
        
        // Loop through properties.
        final Iterator<VertexProperty<Object>> properties = summary.properties();
        while (properties.hasNext()) {
            VertexProperty<Object> property = properties.next();
            System.out.println(property.key() + ": " + property.value());
        }
```
Will produce the following output:
```
vertex_count_per_label: {person=3, cat=1}
edge_count_per_label: {swonk=1, owns=1, knows=3}
vertex_properties_per_label: {person=[name, location, age], cat=[breed]}
vertex_count: 4
edge_count: 5
edge_properties_per_label: {swonk=[], owns=[], knows=[weight, since]}
```
Alternatively the call step can be used to retrieve data.
The following is a sample of the call step:
```
        // Add vertices and edges.
        final Vertex lyndon = g.addV("person").property("name", "Lyndon Bauto").property("age", 30).next();
        final Vertex simon = g.addV("person").property("name", "Simon Zhao").property("location", "Vancouver").next();
        final Vertex grant = g.addV("person").property("name", "Grant Haywood").next();
        final Vertex mycroft = g.addV("cat").property("breed", "Maine Coone").next();
        g.addE("knows").from(lyndon).to(simon).property("weight", 1.0).iterate();
        g.addE("knows").from(lyndon).to(grant).property("since", "2022").iterate();
        g.addE("knows").from(simon).to(grant).iterate();
        g.addE("swonk").from(grant).to(lyndon).iterate();
        g.addE("owns").from(lyndon).to(mycroft).iterate();
        
        // Add a sleep to allow the summary task to run.
        Thread.sleep(1000);
        
        // Get summary vertex.
        final Object summary = g.call("summary").next();
        System.out.println(summary);
```
Will produce the following output (note summary Object above is a Map):
```
{Edge properties by label={swonk=[], owns=[], knows=[weight, since]}, Total vertex count=4, Vertex count by label={person=3, cat=1}, Vertex properties by label={person=[name, location, age], cat=[breed]}, Total edge count=5, Edge count by label={swonk=1, owns=1, knows=3}}
```
And a final way to access the summary data with pretty print support:
```
        // Add vertices and edges.
        final Vertex lyndon = g.addV("person").property("name", "Lyndon Bauto").property("age", 30).next();
        final Vertex simon = g.addV("person").property("name", "Simon Zhao").property("location", "Vancouver").next();
        final Vertex grant = g.addV("person").property("name", "Grant Haywood").next();
        final Vertex mycroft = g.addV("cat").property("breed", "Maine Coone").next();
        g.addE("knows").from(lyndon).to(simon).property("weight", 1.0).iterate();
        g.addE("knows").from(lyndon).to(grant).property("since", "2022").iterate();
        g.addE("knows").from(simon).to(grant).iterate();
        g.addE("swonk").from(grant).to(lyndon).iterate();
        g.addE("owns").from(lyndon).to(mycroft).iterate();
        
        // Add a sleep to allow the summary task to run.
        Thread.sleep(1000);
        
        // Get summary vertex.
        final Object summary = g.call("summary").with("pretty").next();
        System.out.println(summary);
```
Will produce the following output:
```
Total vertex count: 4.
Vertex count by label: {person=3, cat=1}.
Vertex properties by label: {person=[name, location, age], cat=[breed]}.
Total edge count: 5.
Edge count by label: {swonk=1, owns=1, knows=3}.
Edge properties by label: {swonk=[], owns=[], knows=[weight, since]}.
```