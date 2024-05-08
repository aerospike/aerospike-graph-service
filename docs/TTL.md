# Time to Live (TTL)

Aerospike Graph Service supports TTL on Vertices and Edges. In order to assign 
a TTL to an element, assign it a virtual property using the key `~ttl` and a 
numeric value corresponding to how long the element should live in seconds 
before it is automatically dropped from the graph. When an element expires 
via TTL, the behaviour will have identical side effects as a normal element 
drop, e.g. if a Vertex expires, all attached Edges to said Vertex will also be 
dropped.

## How to Enable TTL

In order to enable the TTL feature, the following settings must be configured 
in the `.properties` file used for Aerospike Graph Service.

```
aerospike.graph.ttl.enabled=true
aerospike.graph.ttl.purge.interval=2
```

* `aerospike.graph.ttl.enabled`
  * Type: boolean
  * Default: false
  * Description: Whether the TTL feature is enabled for Aerospike Graph Service.
  * Notes: Enabling this feature creates a background worker thread on the
    Aerospike Graph Service machine and an additional secondary index on 
    Vertices and Edges.
* `aerospike.graph.ttl.purge.interval`
  * Type: int
  * Default: 2
  * Description: Interval between scheduling future TTL purges in seconds. 
  * Notes: A higher value will decrease processing overhead on the Aerospike
  * Graph Service machine. A lower value will decrease the amount of possible
  * delay for an element expired via TTL to be removed. However, a lower value
  * will not help reduce removal delays if they are caused by an excessive amount
  * of elements expiring within an interval.

## Usage

To assign a TTL to an element, assign it a property with the key `~ttl` via a 
regular Gremlin traversal for adding properties. The value must be numeric and 
is the amount of seconds for which the element should live from creation until 
it expires and is dropped from the graph.

```java
// Create a Vertex with a TTL of 10 hours.
g.addV("v1").property("~ttl", 36000).iterate();

// Adding a TTL to an existing Vertex.
g.addV("v2").iterate();
g.V().hasLabel("v2").property("~ttl", 36000).iterate();

// Updating a TTL of an existing Vertex with TTL. (10 hours -> 5 hours)
g.V().hasLabel("v1").property("~ttl", 18000).iterate();
```
