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

| Config                                     | Default        | Description                                                                                                     |
|--------------------------------------------|----------------|-----------------------------------------------------------------------------------------------------------------|
| aerospike.client.host                      | localhost:3000 | Accessible address of one or more an Aerospike seed nodes. To specify multiple, use a comma-separated list.     |
| aerospike.client.port                      |                |                                                                                                                 |
| aerospike.client.timeout                   | 2000           | Timeout assigned to the Aerospike client (in milliseconds).                                                     |
| aerospike.client.user                      | _none_         | Username to use when connecting to the Aerospike cluster.                                                       |
| aerospike.client.password                  | _none_         | Password to use when connecting to the Aerospike cluster.                                                       |
| aerospike.client.namespace                 | test           | Namespace to use for storage of graph data. Note: This namespace must already exist on the Aerospike cluster.   |
| aerospike.client.max.connections.per.node  | 2000           | Max number of connections for a client to have open on a per Aerospike node basis.                              |
| aerospike.client.scan.max.wait             | 2000           | Max time to allow a scan to be idle before aborting (in milliseconds).                                          |
| aerospike.client.batch.read.size           | 5000           | Max number of elements to batch read when reading a stream of elements (default max in Aerospike is 5000).      |
| aerospike.client.connection.max.retry      | 10             | Max number of attempts to connect to the Aerospike cluster.                                                     |
| aerospike.client.write.max.retry           | 100            | Max number of times to retry a write to the Aerospike cluster.                                                  |
| aerospike.client.tls                       | false          | Enable TLS.                                                                                                     |
| aerospike.graph.log.level                  | INFO           | Log level for the Aerospike Graph instance.                                                                     |
| aerospike.graph.data.model                 | packed         | Backend data model of the Aerospike Graph instance.                                                             |
| aerospike.graph.index.adjacency.enabled    | false          | Enable adjacency indexes on the Aerospike Graph instance.                                                       |
| aerospike.graph.index.vertex.label.enabled | false          | Enable vertex label indexes on the Aerospike Graph instance.                                                    |
| aerospike.graph.index.edge.label.enabled   | false          | Enable edge label indexes on the Aerospike Graph instance. (Current not implemented).                           |
| aerospike.graph.index.vertex.properties    | _none_         | Comma delimited list of vertex properties to create an index on.                                                |
| aerospike.graph.index.edge.properties      | _none_         | Comma delimited list of edge properties to create an index on.                                                  |
| aerospike.graph.summary.enabled            | true           | Enable Aerospike Graph summary metadata.                                                                        |
| aerospike.graph.summary.ticker.enabled     | true           | Enable Aerospike Graph summary metadata ticker.                                                                 |
| aerospike.graph.phat.edge.size             | 10             | Number of edges to pack into a single record.                                                                   |
| aerospike.graph.id                         | _none_         | Specify a unique identifier for this graph database. Multiple instances of Aerospike Graph can use the same id. |

## Hybrid Configs
| Config                                                | Default | Description                                                 | 
|-------------------------------------------------------|---------|-------------------------------------------------------------|
| aerospike.graph.cache.weight                          | 1000000 | Max cache weight.                                           | 
| aerospike.graph.metadata.index.update.frequency       | 30000   | Update frequency of index metadata (in milliseconds).       |
| aerospike.graph.metadata.cardinality.update.frequency | 3600000 | Update frequency of cardinality metadata (in milliseconds). |
| aerospike.graph.strategy.fast.count.enabled           | true    | Enable fast count strategy.                                 |
| aerospike.graph.strategy.cache.read.through.enabled   | true    | Enable read through cache strategy.                         |
| aerospike.graph.strategy.prefetch.enabled             | false   | Enable prefetch strategy. Currently not implemented.        |
| aerospike.graph.strategy.drop.enabled                 | true    | Enable drop strategy.                                       |
| aerospike.graph.strategy.composite.id.enabled         | true    | Enable composite id strategy.                               |
| aerospike.graph.strategy.batch.edge.read.enabled      | true    | Enable batch read strategy.                                 |
| aerospike.graph.global.edge.cache.enabled             | true    | Enable edge cache globally.                                 |
| aerospike.graph.vertex.id.buffer.size                 | 1000    | Buffer size of id manager for vertices.                     |
| aerospike.graph.edge.id.buffer.size                   | 10000   | Buffer size of id manager for edges.                        |
| aerospike.graph.property.id.buffer.size               | 10000   | Buffer size of id manager for properties.                   |
