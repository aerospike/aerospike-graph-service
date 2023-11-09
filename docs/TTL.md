# Time to Live (TTL)

Aerospike Graph Service supports TTL on Vertices and Edges. In order to assign 
a TTL to an element, assign it a virtual property using the key `~ttl` and a 
numeric value corresponding to how long the element should live before it is 
automatically dropped from the graph in milliseconds. When an element expires 
via TTL, the behaviour will have identical side effects as a normal element 
drop, e.g. if a Vertex expires, all attached Edges to said Vertex will also be 
dropped.

## How to Enable TTL

In order to enable the TTL feature, the following settings must be configured 
in the `.properties` file used for Aerospike Graph Service.

```
aerospike.graph.ttl.enabled=true
aerospike.graph.ttl.purge.interval=300000
```

* aerospike.graph.ttl.enabled
  * Type: boolean
  * Default: false
  * Description: Whether the TTL feature is enabled for Aerospike Graph Service.
  * Notes: Enabling this feature creates background worker threads on the
    Aerospike Graph Service machine and an additional secondary index on 
    Vertices and Edges.
* aerospike.graph.ttl.purge.interval
  * Type: int
  * Default: 300000
  * Description: Interval between scheduling future TTL purges in milliseconds. 
  * Notes: A higher value will use more RAM on the Aerospike Graph Service 
    machine but reduce processing overhead and reads to the database. This is 
    also the amount of time from an element's TTL in which updating its TTL 
    cannot be guaranteed (see below).

## Usage

To assign a TTL to an element, assign it a property with the key `~ttl` via a 
regular Gremlin traversal for adding properties. The value must be numeric and 
is the amount of milliseconds for which the element should live from creation
until it expires and is dropped from the graph.

```java
// Create a Vertex with a TTL of one minute.
Vertex v1 = g.addV("v1").property("~ttl", 60000).next();

// Adding a TTL to an existing Vertex.
Vertex v2 = g.addV("v2").next();
g.V().hasLabel("v2").property("~ttl", 30000).iterate();

// Updating a TTL of an existing Vertex with TTL.
g.V().hasLabel("v1").property("~ttl", 120000).iterate();
```

### Updating TTL and the aerospike.graph.ttl.purge.interval Configuration

In the following situations related to the time specified by the 
aerospike.graph.ttl.purge.interval configuration, the TTL of an element cannot 
be updated.

1. If the value of the `~ttl` set is lesser or equal to 
   `aerospike.graph.ttl.purge.interval` at any time, e.g. via creation or update.
2. If the *expiry* time of the element is within the *current* time plus the 
   duration specified by `aerospike.graph.ttl.purge.interval`.

#### Example

The purge interval is set to one minute.

`aerospike.graph.ttl.purge.interval=60000`

```java
// Create a Vertex with a TTL of thirty seconds.
Vertex v1 = g.addV("v1").property("~ttl", 30000).next();
// Update the TTL to two minutes from now even though scenario #1 applies.
g.V().hasLabel("v1").property("~ttl", 120000).iterate();
// Wait thirty seconds and look for v1.
Thread.sleep(30000);
g.V().hasLabel("v1").hasNext(); // Returns false.

// Create a Vertex with a TTL of two minutes.
Vertex v2 = g.addV("v2").property("~ttl", 120000);
// Wait thirty seconds and update the TTL to be two minutes again.
Thread.sleep(30000);
g.V().hasLabel("v2").property("~ttl", 120000);
// Wait ninety seconds and look for v2 (two minutes total since start).
Thread.sleep(90000);
g.V().hasLabel("v2").hasNext(); // Returns true. 
// Note that the expiry time of v2 is now in thirty seconds, which falls under 
// scenario #2. Update the TTL anyways.
g.V().hasLabel("v2").property("~ttl", 120000);
Thread.sleep(30000);
g.V().hasLabel("v2").hasNext(); // Returns false.
```
