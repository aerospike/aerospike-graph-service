# Configuration options

Key `aerospike.*` properties recognized by the service. Options are
grouped by audience:

- **Common**: the options most deployments set. Start here.
- **Gremlin Server**: knobs passed straight through to the embedded
  Apache TinkerPop Gremlin Server.
- **Advanced**: tunables where the default is usually right; change
  them only if you have a concrete reason.

> This doc intentionally does not list purely implementation-internal
> switches (bin names, experimental feature flags, etc.). Those live
> next to the code that reads them and should not be relied on by
> user configuration: they can be renamed or removed without notice.

## Common

| Config                                     | Default   | Allowed Values                                                      | Description                                                                                                                          |
| ------------------------------------------ | --------- | ------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| aerospike.client.host                      | localhost | Hostname or comma-separated hostnames.                              | Accessible address of one or more Aerospike seed nodes. Configure the port separately with `aerospike.client.port` (default `3000`). |
| aerospike.client.user                      | _none_    | String values, must match a user configured for Aerospike database. | Username to use when connecting to the Aerospike cluster.                                                                            |
| aerospike.client.password                  | _none_    | String values, must match Aerospike password for provided user.     | Password to use when connecting to the Aerospike cluster.                                                                            |
| aerospike.client.namespace                 | test      | String values, must match Aerospike namespace String.               | Namespace to use for storage of graph data. Note: This namespace must already exist on the Aerospike cluster.                        |
| aerospike.client.tls                       | false     | true, false                                                         | Enable TLS.                                                                                                                          |
| aerospike.graph.log.level                  | INFO      | OFF, ERROR, WARN, INFO, DEBUG, TRACE, ALL                           | Log level for the Aerospike Graph instance.                                                                                          |
| aerospike.graph.index.vertex.label.enabled | false     | true, false                                                         | Enable vertex label indexes on the Aerospike Graph instance.                                                                         |
| aerospike.graph.index.vertex.properties    | _none_    | Any String.                                                         | Comma delimited list of vertex properties to create an index on.                                                                     |
| aerospike.graph.summary.enabled            | true      | true, false                                                         | Enable Aerospike Graph summary metadata.                                                                                             |
| aerospike.graph.summary.ticker.enabled     | true      | true, false                                                         | Enable Aerospike Graph summary metadata ticker.                                                                                      |

## Gremlin Server configs

Gremlin server configs are exposed as aerospike.graph-service.* in the properties file. These configs are injected into gremlin-server.
Min and max heap can also be configured here.

| Config                                        | Default                                         | Allowed Values                                | Description                                                                                                                |
| --------------------------------------------- | ----------------------------------------------- | --------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| aerospike.graph-service.heap.min              | None                                            | String that follows JAVA_OPTIONS xms standard | Minimum heap size                                                                                                          |
| aerospike.graph-service.heap.max              | 80% container memory (via -XX:MaxRAMPercentage) | String that follows JAVA_OPTIONS xmx standard | Maximum heap size. When unset, the JVM automatically uses 80% of the container's memory limit.                             |
| aerospike.graph-service.port                  | 8182                                            | Any Integer                                   | https://tinkerpop.apache.org/javadocs/current/full/org/apache/tinkerpop/gremlin/server/Settings.html#port                  |
| aerospike.graph-service.threadPoolWorker      | 8                                               | Any Integer                                   | https://tinkerpop.apache.org/javadocs/current/full/org/apache/tinkerpop/gremlin/server/Settings.html#threadPoolWorker      |
| aerospike.graph-service.gremlinPool           | availableProcessors()                           | Any Integer                                   | https://tinkerpop.apache.org/javadocs/current/full/org/apache/tinkerpop/gremlin/server/Settings.html#gremlinPool           |
| aerospike.graph-service.evaluationTimeout     | 10000                                           | Any Integer                                   | https://tinkerpop.apache.org/javadocs/current/full/org/apache/tinkerpop/gremlin/server/Settings.html#evaluationTimeout     |
| aerospike.graph-service.idleConnectionTimeout | 0                                               | Any Integer                                   | https://tinkerpop.apache.org/javadocs/current/full/org/apache/tinkerpop/gremlin/server/Settings.html#idleConnectionTimeout |


## Advanced

| Config                                                      | Default | Description                                                                                                                                                              |
| ----------------------------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| aerospike.client.scan.max.wait                              | 2000    | Max time to allow a scan to be idle before aborting (in milliseconds).                                                                                                   |
| aerospike.client.batch.read.size                            | 0       | Maximum number of elements in a batch read. When `0`, the effective size is calculated from the number of nodes and the per-node setting.                                |
| aerospike.graph.id                                          | _none_  | Specify a unique identifier for this graph database. Multiple instances of Aerospike Graph can use the same id.                                                          |
| aerospike.graph.phat.edge.size                              | 10      | Number of edges to pack into a single record.                                                                                                                            |
| aerospike.graph.cache.weight                                | 1000000 | Max cache weight.                                                                                                                                                        |
| aerospike.graph.admin.metadata.index.update.frequency       | 30000   | Update frequency of index metadata (in milliseconds).                                                                                                                    |
| aerospike.graph.admin.metadata.cardinality.update.frequency | 3600000 | Update frequency of cardinality metadata (in milliseconds).                                                                                                              |
| aerospike.graph.strategy.fast.count.enabled                 | true    | Enable fast count strategy.                                                                                                                                              |
| aerospike.graph.strategy.cache.read.through.enabled         | true    | Enable read through cache strategy.                                                                                                                                      |
| aerospike.graph.strategy.prefetch.enabled                   | true    | Enable prefetch strategy.                                                                                                                                                |
| aerospike.graph.strategy.drop.enabled                       | true    | Enable drop strategy.                                                                                                                                                    |
| aerospike.graph.strategy.composite.id.enabled               | true    | Enable composite id strategy.                                                                                                                                            |
| aerospike.graph.strategy.batch.edge.read.enabled            | true    | Enable batch read strategy.                                                                                                                                              |
| aerospike.graph.global.edge.cache.enabled                   | true    | Enable edge cache globally.                                                                                                                                              |
| aerospike.graph.vertex.id.buffer.size                       | 1000    | Buffer size of id manager for vertices.                                                                                                                                  |
| aerospike.graph.edge.id.buffer.size                         | 10000   | Buffer size of id manager for edges.                                                                                                                                     |
| aerospike.graph.property.id.buffer.size                     | 10000   | Buffer size of id manager for properties.                                                                                                                                |
| aerospike.client.policy.maxRetries                          | 2       | Max number of times to retry a write to the Aerospike cluster. Use with care: graph writes are not all idempotent, so retries can produce duplicate edges in edge cases. |
