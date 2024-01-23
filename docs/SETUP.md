<img  src="img/firefly_10kft.drawio.png">

Firefly is implemented as a JVM process. 
Each Firefly node will establish a connection to one or more Aerospike nodes, by configuration.

Firefly is hosted by `gremlin-server` which exposes a Websocket endpoint for client applications
to connect to. Traversals submitted to this `gremlin-server` socket will be invoked on the specific
Firefly node being hosted by it, which will dispatch queries to the Aerospike cluster at large.

In the configuration file for each Firefly node, you may specify one or more Aerospike servers to connect to

#### One

```properties
gremlin.graph=com.aerospike.firefly.structure.FireflyGraph
aerospike.client.host=172.17.0.1
aerospike.client.port=3000
aerospike.client.namespace=test
aerospike.graph.data.model=packed
```
#### Many

```properties
gremlin.graph=com.aerospike.firefly.structure.FireflyGraph
aerospike.client.host=172.18.0.3:3000,172.18.0.2:3000,172.18.0.4:3000
aerospike.client.namespace=test
firefly_data_model=packed
```

In this configuration file, you will also specify the `data model` and Aerospike `namespace`

currently supported data models are:
 - linked
 - packed

The namespace must be configured on your Aerospike cluster ahead of time.

`gremlin-server` is configured via a yaml file, which specifies the properties file to use with Firefly.  This is abstracted from the user. If it needs to be overwritten, a `aerospike-graph-service.yaml` can be placed at 
`/opt/aerospike-graph/custom/aerospike-graph-service.yaml`.