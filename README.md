Firefly
-----------
Firefly is an [Apache TinkerPop3®](http://tinkerpop.apache.org) compliant graph database, backed by [Aerospike Enterprise®](https://aerospike.com/products/features-and-editions/).

<img src="https://raw.githubusercontent.com/apache/tinkerpop/master/docs/static/images/tinkerpop-character.png" alt="TinkerPop" width="100"/>

Running Firefly Through Docker
-----------
### Local Aerospike Instance

If you are running Aerospike locally, you can use the baked in quickstart configuration for Firefly. This is absolutely not recommended for production use.

/opt/aerospike-firefly/conf/firefly-gremlin-server.yaml is the configuration file for the Firefly gremlin-server that is baked into docker with some default configurations.

Note: *Replace <VERSION> with the version you would like.*
```
docker run -t -i -p8182:8182 --entrypoint gremlin-server.sh ghcr.io/citrusleaf/firefly:<VERSION> /opt/aerospike-firefly/conf/firefly-gremlin-server.yaml
```

### Remote Aerospike Instance

If Aerospike is running remotely, a path to the directory that contains the configuration file for the Firefly gremlin-server locally must be provided.

This configuration file must also contain the path to firefly-graph.properties, which should be in the same directory.

To pass this to the docker container, replace the <PATH_TO_CONFIGURATION> in the command below with the absolute path of the directory.

```
docker run -v <PATH_TO_CONFIGURATION>:/opt/aerospike-firefly/local/conf -t -i -p8182:8182 --entrypoint gremlin-server.sh ghcr.io/citrusleaf/firefly:0.0.0-docker-test /opt/aerospike-firefly/local/conf/firefly-gremlin-server.yaml
```


An example configuration file for `firefly-gremlin-server.yaml` is provided below. Please replace <REPLACE WITH ABSOLUTE PATH TO firefly-graph.properties> with
the absolute path to the firefly-graph.properties file.

Note, adjusting the evaluationTimeout, among other parameters, is sometimes useful.

```
host: 0.0.0.0
port: 8182
evaluationTimeout: 30000
channelizer: org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer
graphs: {
graph: <REPLACE WITH ABSOLUTE PATH TO firefly-graph.properties>}

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

An example configuration file for `firefly-graph.properties` is provided below.

Please replace <AEROSPIKE_IP_ADDRESS> with the ip address of your Aerospike cluster.

```
gremlin.graph=com.aerospike.firefly.structure.FireflyGraph
aerospike_host=<AEROSPIKE_IP_ADDRESS>
aerospike_port=3000
aerospike_namespace=test
```