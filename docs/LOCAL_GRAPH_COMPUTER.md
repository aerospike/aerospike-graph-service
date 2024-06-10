## Online Analytical Processing

Online Analytical Processing (OLAP) is an approach to data processing that assumes a significant portion of the dataset
will be analyzed. As such, OLAP exists on the opposite side of the data processing spectrum than OLTP (only
transactional processing). The following table provides a

|  OLTP <br/>(Online Transactional Processing) | OLAP <br/>(Online Analytical Processing) |
|:---------------------------------------------|:-----------------------------------------|
| many concurrent processes                    | a single or few concurrent processes     |
| process limited subset of the graph          | process large subset of the graph        |
| 'pointer chasing' execution                  | linear-scan/join execution               |
| short running queries                        | long running queries                     |
| results on the order of milliseconds         | results on the order of seconds to hours |    

### Graph OLAP with GraphComputer

The `GraphComputer` interface enables Gremlin to be used for OLAP graph processing.

> #### IMPORTANT
> - The Gremlin language can be executed using OLTP and OLAP processors.
> - There are some limitations when executing OLAP Gremlin traversals.
> - Gremlin OLTP is single threaded and Gremlin OLAP is multi-threaded.

A collection of common Gremlin OLAP queries are presented below:

| Query                                                                  | Description                                  |
|:-----------------------------------------------------------------------|:---------------------------------------------|
| `g.V().count()`                                                        | count the number of vertices in the graph    |
| `g.V().groupCount().by('zipcode')`                                     | count the number of vertices in each zipcode |
| `g.V().out('rated').count()`                                           | count the number of ratings in the graph     |
| `g.V().outE('rated').value('stars').mean()`                            | the average rating value in the graph        |
| `g.V().out('rated')`<br/>&nbsp;`out('vendor').groupCount().by('name')` | count the number of ratings each vendor has for their products |                             

### LocalGraphComputer

AerospikeGraph exposes OLAP graph processing capabilities through the `GraphComputer` interface. Currently,
AerospikeGraph provides a single `GraphComputer` implementation called `LocalGraphComputer`. This OLAP processor
executes on a single node and is oriented for processing small to medium-sized subgraphs in a multi-threaded manner.

```
gremlin> olap = graph.traversal().withComputer(LocalGraphComputer)
gremlin> olap.V()
gremlin> olap.V().groupCount().by('zipcode')
```
