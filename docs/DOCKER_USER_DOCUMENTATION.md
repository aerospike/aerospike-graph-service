Aerospike Graph
-----------
Aerospike Graph is an [Apache TinkerPop3®](http://tinkerpop.apache.org) compliant graph database, backed by [Aerospike Enterprise®](https://aerospike.com/products/features-and-editions/).

<img src="https://raw.githubusercontent.com/apache/tinkerpop/master/docs/static/images/tinkerpop-character.png" alt="TinkerPop" width="100"/>

Running Aerospike Graph Through Docker
-----------
### With Environment Variables (simple quickstart)

The simplest way to run Firefly is to use the docker image with environment variables. This technique only allows a few 
variables to be set and is not recommended for production. 

A sample where the Aerospike cluster's ip addresses are `aerospike-devel-cluster-host1` and 
`aerospike-devel-cluster-host2` is provided below. Additionally, in this example we configur the graph to have
vertex property indexes on `property1` and `property2` and a vertex label index enabled.
The namespace is set to `test`.

```
docker run -p8182:8182 -e aerospike_namespace="test" -e \
AEROSPIKE_HOST="aerospike-devel-cluster-host1:3000, aerospike-devel-cluster-host2:3000" \
-e aerospike.graph.index.vertex.properties=property1,property2 \
-e aerospike.graph.index.vertex.label.enabled=true ghcr.io/citrusleaf/firefly 
```

### With a Properties File (recommended for most production cases)

The docker container can also be started with a properties file. This is the recommended way to run Aerospike Graph
in production for most cases that do not require more advanced server configuration. The docker container is started
the properties file as shown below:

```
docker run -p 8182:8182 -v /home/graph-user/graph/conf/firefly-graph.properties:/opt/aerospike-firefly/conf/firefly-graph.properties ghcr.io/citrusleaf/firefly
```

Where `/home/graph-user/graph/conf/firefly-graph.properties` is the path to the properties file on the host machine.

An example properties file is provided below:

```
gremlin.graph=com.aerospike.firefly.structure.FireflyGraph
aerospike.client.host=aerospike-devel-cluster-host1:3000, aerospike-devel-cluster-host2:3000
aerospike.client.namespace=test
aerospike.graph.index.vertex.label.enabled=true
aerospike.graph.index.vertex.properties=property1,property2
```

### With a YAML and Properties File (recommended for advanced server configuration)

Gremlin server has support for a yaml file that allows advanced configuration. This yaml file works in conjunction
with the properties file to configure the server properties as well as the Graph computing services properties.

To pass in a yaml file, the docker container is started as shown below:

```
docker run -p 8182:8182 -v /home/graph-user/graph/conf:/opt/aerospike-firefly/conf ghcr.io/citrusleaf/firefly
```

For this command to execute properly, the host machine must have two files present at `/home/graph-user/graph/conf`.

The files present must be a properties file named `firefly-graph.properties` and a yaml file named `gremlin-server.yaml`.

An example `firefly-graph.properties` file is provided above already, and a sample `gremlin-server.yaml` is provided below:

```
host: 0.0.0.0
port: 8182
evaluationTimeout: 1200000
channelizer: org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer
graphs: {
  graph: /opt/aerospike-firefly/conf/firefly-graph.properties}

scriptEngines: {
  gremlin-groovy: {
    plugins: { org.apache.tinkerpop.gremlin.server.jsr223.GremlinServerGremlinPlugin: {},
               org.apache.tinkerpop.gremlin.tinkergraph.jsr223.TinkerGraphGremlinPlugin: {},
               org.apache.tinkerpop.gremlin.jsr223.ImportGremlinPlugin: {classImports: [java.lang.Math], methodImports: [java.lang.Math#*]},
               org.apache.tinkerpop.gremlin.jsr223.ScriptFileGremlinPlugin: {files: [scripts/empty-sample.groovy]}}}}
serializers:
  - { className: org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0, config: { ioRegistries: [org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3d0] }}        # application/json
  - { className: org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1 }                                                                                                           # application/vnd.graphbinary-v1.0
  - { className: org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1, config: { serializeResultToString: true }}                                                                 # application/vnd.graphbinary-v1.0-stringd
processors:
  - { className: org.apache.tinkerpop.gremlin.server.op.session.SessionOpProcessor, config: { sessionTimeout: 28800000 }}
  - { className: org.apache.tinkerpop.gremlin.server.op.traversal.TraversalOpProcessor, config: { cacheExpirationTime: 600000, cacheMaxSize: 1000 }}
metrics: {
  consoleReporter: {enabled: true, interval: 180000},
  csvReporter: {enabled: true, interval: 180000, fileName: /tmp/gremlin-server-metrics.csv},
  jmxReporter: {enabled: true},
  slf4jReporter: {enabled: true, interval: 180000}}
strictTransactionManagement: false
idleConnectionTimeout: 0
keepAliveInterval: 0
maxInitialLineLength: 4096
maxHeaderSize: 8192
maxChunkSize: 8192
maxContentLength: 10485760
maxAccumulationBufferComponents: 1024
resultIterationBatchSize: 64
writeBufferLowWaterMark: 32768
writeBufferHighWaterMark: 65536
ssl: {
  enabled: false}
```

It is important to note the property `graphs: { graph: /opt/aerospike-firefly/conf/firefly-graph.properties}`
in the yaml file. This is required for the server to know where to find the properties file which was pathed 
into the container there.

The documentation for the configuration options for the `gremlin-server.yaml` are available on TinkerPop
's website https://tinkerpop.apache.org/docs/current/reference/#_configuring_2.
