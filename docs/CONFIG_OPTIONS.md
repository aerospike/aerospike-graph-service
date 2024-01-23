# Configuration Options
Configuration options are broken into 3 sections:
- External configs
  - These are configs that all customers should be aware of eventually, we will likely limit these to a subset for 1.0
  - These are configs like `aerospike.client.host`
- Hybrid configs
  -  These are configs that _some_ customers should be aware of
  - These are configs like `aerospike.graph.cache.weight` where some use cases may want a large cache
- Internal configs
  - These are configs that no customers should be aware of
  - These are configs like what we name the bin which we place type hints

## External Configs

| Config                                     | Default        | Allowed Values                                                      | Description                                                                                                        |
|--------------------------------------------|----------------|---------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| aerospike.client.host                      | localhost:3000 | Any String that follows `<host>:<port>` format.                     | Accessible address of one or more an Aerospike seed nodes, specified as `<host>:<port>` in a comma-separated list. |
| aerospike.client.timeout                   | 2000           | 1 - 100000                                                          | Timeout assigned to the Aerospike client (in milliseconds).                                                        |
| aerospike.client.user                      | _none_         | String values, must match a user configured for Aerospike database. | Username to use when connecting to the Aerospike cluster.                                                          |
| aerospike.client.password                  | _none_         | String values, must match Aerospike password for provided user.     | Password to use when connecting to the Aerospike cluster.                                                          |
| aerospike.client.namespace                 | test           | String values, must match Aerospike namespace String.               | Namespace to use for storage of graph data. Note: This namespace must already exist on the Aerospike cluster.      |
| aerospike.client.tls                       | false          | true, false                                                         | Enable TLS.                                                                                                        |
| aerospike.graph.log.level                  | INFO           | OFF, ERROR, WARN, INFO, DEBUG, TRACE, ALL                           | Log level for the Aerospike Graph instance.                                                                        |
| aerospike.graph.index.vertex.label.enabled | false          | true, false                                                         | Enable vertex label indexes on the Aerospike Graph instance.                                                       |
| aerospike.graph.index.vertex.properties    | _none_         | Any String.                                                         | Comma delimited list of vertex properties to create an index on.                                                   |
| aerospike.graph.summary.enabled            | true           | true, false                                                         | Enable Aerospike Graph summary metadata.                                                                           |
| aerospike.graph.summary.ticker.enabled     | true           | true, false                                                         | Enable Aerospike Graph summary metadata ticker.                                                                    |

## Gremlin Server configs

Gremlin server configs are exposed as aerospike.graph-service.* in the properties file. These configs are injected into gremlin-server.
Min and max heap can also be configured here.

| Config                                        | Default               | Allowed Values                                | Description                                                                                                                 |
|-----------------------------------------------|-----------------------|-----------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| aerospike.graph-service.min-heap              | None                  | String that follows JAVA_OPTIONS xms standard | Minimum heap size                                                                                                           |
| aerospike.graph-service.max-heap              | 80% available memory  | String that follows JAVA_OPTIONS xmx standard | Maximum heap size                                                                                                           |
| aerospike.graph-service.port                  | 8182                  | Any Integer                                   | https://tinkerpop.apache.org/javadocs/current/full/org/apache/tinkerpop/gremlin/server/Settings.html#port                   |
| aerospike.graph-service.threadPoolWorker      | 8                     | Any Integer                                   | https://tinkerpop.apache.org/javadocs/current/full/org/apache/tinkerpop/gremlin/server/Settings.html#threadPoolWorker       |
| aerospike.graph-service.gremlinPool           | availableProcessors() | Any Integer                                   | https://tinkerpop.apache.org/javadocs/current/full/org/apache/tinkerpop/gremlin/server/Settings.html#gremlinPool            |
| aerospike.graph-service.evaluationTimeout     | 10000                 | Any Integer                                   | https://tinkerpop.apache.org/javadocs/current/full/org/apache/tinkerpop/gremlin/server/Settings.html#evaluationTimeout      |
| aerospike.graph-service.idleConnectionTimeout | 0                     | Any Integer                                   | https://tinkerpop.apache.org/javadocs/current/full/org/apache/tinkerpop/gremlin/server/Settings.html#idleConnectionTimeout  |


## Hybrid Configs

| Config                                                | Default | Description                                                                                                                                                   |
|-------------------------------------------------------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| aerospike.client.scan.max.wait                        | 2000    | Max time to allow a scan to be idle before aborting (in milliseconds).                                                                                        |
| aerospike.client.batch.read.size                      | 5000    | Max number of elements to batch read when reading a stream of elements (default max in Aerospike is 5000).                                                    |
| aerospike.graph.id                                    | _none_  | Specify a unique identifier for this graph database. Multiple instances of Aerospike Graph can use the same id.                                               |
| aerospike.graph.phat.edge.size                        | 10      | Number of edges to pack into a single record.                                                                                                                 |
| aerospike.graph.index.adjacency.enabled               | true    | Enable adjacency indexes on the Aerospike Graph instance. This allows fast traversing across the edges of supernodes but increases memory usage of Aerospike. |
| aerospike.graph.cache.weight                          | 1000000 | Max cache weight.                                                                                                                                             | 
| aerospike.graph.metadata.index.update.frequency       | 30000   | Update frequency of index metadata (in milliseconds).                                                                                                         |
| aerospike.graph.metadata.cardinality.update.frequency | 3600000 | Update frequency of cardinality metadata (in milliseconds).                                                                                                   |
| aerospike.graph.strategy.fast.count.enabled           | true    | Enable fast count strategy.                                                                                                                                   |
| aerospike.graph.strategy.cache.read.through.enabled   | true    | Enable read through cache strategy.                                                                                                                           |
| aerospike.graph.strategy.prefetch.enabled             | false   | Enable prefetch strategy. Currently not implemented.                                                                                                          |
| aerospike.graph.strategy.drop.enabled                 | true    | Enable drop strategy.                                                                                                                                         |
| aerospike.graph.strategy.composite.id.enabled         | true    | Enable composite id strategy.                                                                                                                                 |
| aerospike.graph.strategy.batch.edge.read.enabled      | true    | Enable batch read strategy.                                                                                                                                   |
| aerospike.graph.global.edge.cache.enabled             | true    | Enable edge cache globally.                                                                                                                                   |
| aerospike.graph.vertex.id.buffer.size                 | 1000    | Buffer size of id manager for vertices.                                                                                                                       |
| aerospike.graph.edge.id.buffer.size                   | 10000   | Buffer size of id manager for edges.                                                                                                                          |
| aerospike.graph.property.id.buffer.size               | 10000   | Buffer size of id manager for properties.                                                                                                                     |
| aerospike.client.write.max.retry                      | 0       | (Currently internal until writes are idempotent) Max number of times to retry a write to the Aerospike cluster.                                               |
