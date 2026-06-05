# Time to live (TTL)

## Aerospike namespace requirement

Before you enable graph-element TTL, the Aerospike namespace that AGS
uses must have the [`default-ttl`](https://aerospike.com/docs/database/reference/config#namespace__default-ttl)
configuration option set to `0`. That is a deployment prerequisite,
separate from the graph TTL feature described on this page. See
[TTL on the Aerospike namespace](https://aerospike.com/docs/graph/deploy/docker#ttl-on-the-aerospike-namespace)
and the [official graph documentation](https://aerospike.com/docs/graph).

## Graph-element TTL

Aerospike Graph Service supports TTL on vertices and edges. To assign
a TTL to an element, assign it a virtual property using the key `~ttl` and a
numeric value corresponding to how long the element should live in seconds
before it is automatically dropped from the graph. When an element expires
through TTL, the behavior matches a normal element
drop. For example, if a vertex expires, all attached edges to that vertex are also
dropped.

## How to enable TTL

To enable the TTL feature, configure the following settings in the
`.properties` file used for Aerospike Graph Service.

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
    vertices and edges.
* `aerospike.graph.ttl.purge.interval`
  * Type: int
  * Default: 2
  * Description: Interval between scheduling future TTL purges in seconds.
  * Notes: A higher value decreases processing overhead on the Aerospike
  * Graph Service machine. A lower value decreases the amount of possible
  * delay for an element expired through TTL to be removed. However, a lower value
  * does not help reduce removal delays if they are caused by an excessive amount
  * of elements expiring within an interval.

## Usage

To assign a TTL to an element, assign it a property with the key `~ttl` through a
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
