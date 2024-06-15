## Online Analytical Processing

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

### Graph OLAP with GraphComputer

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
#### OLAP Limitations

1. When evaluating traversals that rely on path information such as `as()/select()`, `path()`, `otherV()`, etc., practical computational limits can easily be reached due the combinatoric explosion of data. With path computing enabled, every traverser is unique and thus, must be enumerated as opposed to being counted/merged.

2. Steps that are concerned with the global ordering of traversers do not have a meaningful representation in OLAP. For example, what does `order()`-step mean when all traversers are being processed in parallel? Even if the traversers were aggregated and ordered, then at the next step they would return to being executed in parallel and thus, in an unpredictable order. When `order()`-like steps are executed at the end of a traversal (i.e the final step).

3. Anonymous traversals (inner traversals) can only traverse to the depth of the current _star graph_ being processed, where a _star graph_ is a vertex, it's properties, incident edges, and the `id` of the adjacent vertices. Thus,
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

### LocalGraphComputer

AerospikeGraph exposes OLAP graph processing capabilities through the `GraphComputer` interface. Currently,
AerospikeGraph provides a single `GraphComputer` implementation called `LocalGraphComputer`. This OLAP processor
executes on a single node and is oriented for processing small to medium-sized subgraphs in a multi-threaded manner. Exposing an OLAP `GraphTraversalSource` versus an OLTP `GraphTraversalSource` is as simple as declaring a `GraphComputer` to use with a traversal.

```
// OTLP
g = graph.traversal()

// OLAP
g = traversal().withRemote(DriverRemoteConnection.using("111.222.333.444", 8182, "o"))
```

From there, the full Gremlin language is available for execution using the underlying OLAP graph processor.

#### Limitations

`LocalGraphComputer` performs all computations on a single node of the graph cluster. The benefit of this architecture are twofold:

1. Less inter-machine communication
2. Less fail over software complexity

The drawback of this architecture is that the graph being analyzed (a subgraph computed from the structure of the traversal) must fit within the memory constraints of the executing node.

