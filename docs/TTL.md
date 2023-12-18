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
aerospike.graph.ttl.purge.interval=300
aerospike.graph.ttl.update.anytime.enabled=false
```

* `aerospike.graph.ttl.enabled`
  * Type: boolean
  * Default: false
  * Description: Whether the TTL feature is enabled for Aerospike Graph Service.
  * Notes: Enabling this feature creates background worker threads on the
    Aerospike Graph Service machine and an additional secondary index on 
    Vertices and Edges.
* `aerospike.graph.ttl.purge.interval`
  * Type: int
  * Default: 300
  * Description: Interval between scheduling future TTL purges in seconds. 
  * Notes: A higher value will use more RAM on the Aerospike Graph Service 
    machine but reduce processing overhead and reads to the database. This is 
    also the amount of time from an element's TTL in which updating its TTL 
    cannot be guaranteed (see below) unless 
    `aerospike.graph.ttl.update.anytime.enabled` is set to `true`.
* `aerospike.graph.ttl.update.anytime.enabled`
  * Type: boolean
  * Default: false
  * Description: Whether elements in the current purge schedule interval can 
    have their TTL value increased.
  * Notes: Enabling this feature will increase processing overhead and 
    substantially increase the amount of reads to the database. It is 
    recommended to keep this disabled and to reduce the value of 
    `aerospike.graph.ttl.purge.interval` instead unless extending TTL of 
    elements right before their expiry is paramount.

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

### Updating TTL and the aerospike.graph.ttl.purge.interval Configuration

Note: If `aerospike.graph.ttl.update.anytime.enabled` is `true` this does not apply.

In the following situations related to the time specified by the 
aerospike.graph.ttl.purge.interval configuration, the TTL of an element cannot 
be increased but can be decreased.

1. The original value of the `~ttl` property was set as less than or equal to the value of
   `aerospike.graph.ttl.purge.interval` at any time, e.g. via creation or update.
2. If the *expiry* time of the element is within the *current* time plus the
   duration specified by `aerospike.graph.ttl.purge.interval`.

#### Example

The purge interval is set to one minute.

`aerospike.graph.ttl.purge.interval=60`

```java
// Create a vertex with a TTL of thirty seconds.
g.addV("v1").property("~ttl", 30).iterate();
// Update the TTL to two minutes from now.
g.V().hasLabel("v1").property("~ttl", 120).iterate();
// Wait thirty seconds and look for v1.
Thread.sleep(30000);
g.V().hasLabel("v1").hasNext(); // Returns false. The vertex expired via TTL despite attempting to update the value.

// Create a vertex with a TTL of two minutes.
g.addV("v2").property("~ttl", 120).iterate();
// Wait thirty seconds and update the TTL to be two minutes again.
Thread.sleep(30000);
g.V().hasLabel("v2").property("~ttl", 120).iterate();
// Wait ninety seconds and look for v2 (two minutes total since start).
Thread.sleep(90000);
g.V().hasLabel("v2").hasNext().iterate(); // Returns true. First TTL update was successful as expected. 
// Note that the expiry time of v2 is now in thirty seconds, which falls under 
// condition #2. Attempt to update the TTL anyways.
g.V().hasLabel("v2").property("~ttl", 120).iterate();
Thread.sleep(30000);
g.V().hasLabel("v2").hasNext(); // Returns false. The vertex expired via TTL despite attempting to update the value.
```
