# Graph OLAP with GraphComputer

[Online Analytical Processing](https://en.wikipedia.org/wiki/Online_analytical_processing) (OLAP) is an approach to data processing that assumes a significant portion of the dataset
will be analyzed. As such, OLAP exists on the opposite side of the data processing spectrum than OLTP ([online
transactional processing](https://en.wikipedia.org/wiki/Online_transaction_processing)). The following table itemizes typical differences between OLTP and OLAP processors.

|  OLTP <br/>(Online Transactional Processing) | OLAP <br/>(Online Analytical Processing) |
|:---------------------------------------------|:-----------------------------------------|
| many concurrent processes                    | a single or few concurrent processes     |
| process limited subset of the graph          | process large subset of the graph        |
| 'pointer chasing' execution                  | linear-scan/join execution               |
| short running queries                        | long running queries                     |
| results on the order of milliseconds         | results on the order of seconds to hours |

## GraphComputer

The [GraphComputer](https://tinkerpop.apache.org/docs/current/reference/#graphcomputer) interface enables Gremlin to be used for OLAP graph processing.

> #### IMPORTANT
> - The Gremlin language can be executed using OLTP and OLAP processors.
> - There are some limitations when executing OLAP Gremlin traversals.
> - Gremlin OLTP is single threaded and Gremlin OLAP is multi-threaded.

A collection of common Gremlin OLAP query motifs are presented below. Note that most OLAP queries yield _analytics_ (reductions to counts, groupings, averages, etc.) and not explicit datum in the database. The reason being, if `g.V().valueMap()` is evaluated, what is returned is a stream of all properties on all vertices in the graph, which for large graphs, besides being a lot of data to move over the wire, is typically not useful information to procure.

| Query                                                                  | Description                                  |
|:-----------------------------------------------------------------------|:---------------------------------------------|
| `g.V().count()`                                                        | count the number of vertices in the graph    |
| `g.V().groupCount().by('zipcode')`                                     | count the number of vertices in each zipcode |
| `g.V().out('rated').count()`                                           | count the number of ratings in the graph     |
| `g.V().outE('rated').value('stars').mean()`                            | the average rating value in the graph        |
| `g.V().out('rated')`<br/>&nbsp;`out('vendor').groupCount().by('name')` | count the number of ratings each vendor has for their products |

## The Aerospike Graph OLAP implementation

AGS exposes OLAP processing through TinkerPop's `GraphComputer`
interface. The implementation is **`DistributedGraphComputer`**, a
Spark-backed processor that runs a `GraphComputer` job across a Spark
cluster reading from the same Aerospike namespace the online service
writes. It lives in the [`aerospike-graph-olap`](../aerospike-graph-olap)
module and is loaded when analytics support is present on the classpath;
`FireflyGraph.compute()` instantiates
`com.aerospike.firefly.olap.structure.DistributedGraphComputer`. If the
analytics jars are not on the classpath (e.g. the standard, non-Analytics
image), calling `compute()` fails with a message directing you to use the
Aerospike Graph Analytics image or a Spark cluster.

> The gremlin module also contains a `LocalGraphComputer` class, but it
> is not a functioning execution path (`submit()` is not implemented)
> and `compute()` does not return it. Single-JVM OLAP is not a supported
> mode; use the distributed Spark-backed computer.

Selecting the OLAP processor for a traversal is done with
`withComputer()`:

```
// OLTP
g = traversal().withRemote(DriverRemoteConnection.using("<AGS-IP>", <AGS-PORT>, "g"))

// OLAP
g = traversal().withRemote(DriverRemoteConnection.using("<AGS-IP>", <AGS-PORT>, "g")).withComputer()
```

For setup (provisioning a Spark cluster on GCP or AWS, submitting the
OLAP jar, and OLAP-specific configuration and query guidance), see the
[Aerospike Graph OLAP module README](../aerospike-graph-olap/README.md).

## OLAP limitations

These are TinkerPop `GraphComputer` semantics and are enforced by a
verification strategy on the distributed computer.

1. Gremlin OLAP is read only. No mutating/write steps can be used in a traversal. A verifying strategy will throw an exception if a mutation is attempted: `The following step is currently not supported on GraphComputer: AddVertexStartStep({label=[thing]})`.
2. When evaluating traversals that rely on path information such as `as()/select()`, `path()`, `otherV()`, etc., practical computational limits can easily be reached due the combinatoric explosion of data. With path computing enabled, every traverser is unique and thus, must be enumerated as opposed to being counted/merged.

3. Steps that are concerned with the global ordering of traversers do not have a meaningful representation in OLAP. For example, what does `order()`-step mean when all traversers are being processed in parallel? Even if the traversers were aggregated and ordered, then at the next step they would return to being executed in parallel and thus, in an unpredictable order. `order()`-like steps are meaningful when executed as the final step of a traversal.

4. Anonymous traversals (inner traversals) can only traverse to the depth of the current _star graph_ being processed, where a _star graph_ is a vertex, it's properties, incident edges, and the `id` of the adjacent vertices. Thus,
    ```
    // filter to only those people who have more than 10 friends
    g.V().where(outE('knows').count().is(gt(10)))
    ```
    the inner `where()`-traversal remains within the boundary of the current vertex's star graph. However,
    ```
    // filter to only those people who have friends that have more than 10 friends
    g.V().where(out('knows').out('knows').count().is(gt(10)))
    ```
   will throw a `ValidationException` as the inner traversal requires the processing of edges incident to the adjacent vertices of the current vertex being processed.
