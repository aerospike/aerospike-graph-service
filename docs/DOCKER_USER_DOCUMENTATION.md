# Running Aerospike Graph Service in Docker

[Apache TinkerPop®][tinkerpop]-compatible graph database backed by
[Aerospike][aerospike].

<img src="https://raw.githubusercontent.com/apache/tinkerpop/master/docs/static/images/tinkerpop-character.png" alt="TinkerPop" width="100" />

> Released user-facing images are published on Docker Hub as
> `aerospike/aerospike-graph-service:<version>` (with a moving `:latest`
> tag). Release-candidate builds are pushed to GitHub Container
> Registry as `ghcr.io/aerospike/firefly:<version>` under the internal
> codename. The product name is **Aerospike Graph Service**; see the
> top of the root README for why the codename is preserved on the dev
> image path.

[tinkerpop]: http://tinkerpop.apache.org
[aerospike]: https://aerospike.com

## 1. Quickstart with environment variables

The simplest way to start the service. Suitable for demos and local
development; not recommended for production because only a subset of
configuration is reachable via env vars.

In this example the Aerospike cluster has two seed nodes at
`aerospike-devel-cluster-host1` and `aerospike-devel-cluster-host2`,
the graph uses namespace `test`, and vertex-property indexes are
configured on `property1` and `property2`:

```bash
docker run -d --name graph \
  -p 8182:8182 \
  -e AEROSPIKE_HOST="aerospike-devel-cluster-host1:3000,aerospike-devel-cluster-host2:3000" \
  -e AEROSPIKE_NAMESPACE="test" \
  -e aerospike.graph.index.vertex.properties=property1,property2 \
  -e aerospike.graph.index.vertex.label.enabled=true \
  aerospike/aerospike-graph-service:latest
```

## 2. Properties file (recommended for most deployments)

Mount a properties file into the container at the path the service
expects:

```bash
docker run -d --name graph \
  -p 8182:8182 \
  -v /host/path/aerospike-graph.properties:/opt/aerospike-graph/conf/aerospike-graph.properties \
  aerospike/aerospike-graph-service:latest
```

Example `aerospike-graph.properties`:

```properties
gremlin.graph=com.aerospike.firefly.structure.FireflyGraph
aerospike.client.host=aerospike-devel-cluster-host1:3000,aerospike-devel-cluster-host2:3000
aerospike.client.namespace=test
aerospike.graph.index.vertex.label.enabled=true
aerospike.graph.index.vertex.properties=property1,property2
```

See [`CONFIG_OPTIONS.md`](CONFIG_OPTIONS.md) for every tunable.

## 3. Properties + custom Gremlin Server YAML (advanced)

For deployments that need to override Gremlin Server internals
(serializers, timeouts, metrics reporters, …), mount a config directory
containing both an `aerospike-graph.properties` and a
`gremlin-server.yaml`:

```bash
docker run -d --name graph \
  -p 8182:8182 \
  -v /host/path/conf:/opt/aerospike-graph/conf \
  aerospike/aerospike-graph-service:latest
```

`/host/path/conf` must contain **both** files:

- `aerospike-graph.properties` — graph-specific options (example above).
- `gremlin-server.yaml` — Gremlin Server options.

Minimal `gremlin-server.yaml` compatible with this deployment:

```yaml
host: 0.0.0.0
port: 8182
evaluationTimeout: 10000
channelizer: org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer
graphs:
  graph: /opt/aerospike-graph/conf/aerospike-graph.properties

scriptEngines: {}
serializers:
  - { className: org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3, config: { ioRegistries: [org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3] }}
  - { className: org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1 }
  - { className: org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1, config: { serializeResultToString: true } }
processors:
  - { className: org.apache.tinkerpop.gremlin.server.op.session.SessionOpProcessor, config: { sessionTimeout: 28800000 } }
  - { className: org.apache.tinkerpop.gremlin.server.op.traversal.TraversalOpProcessor, config: { cacheExpirationTime: 600000, cacheMaxSize: 1000 } }
metrics:
  consoleReporter: { enabled: true, interval: 180000 }
  csvReporter: { enabled: true, interval: 180000, fileName: /tmp/gremlin-server-metrics.csv }
  jmxReporter: { enabled: true }
  slf4jReporter: { enabled: true, interval: 180000 }
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
ssl:
  enabled: false
```

Note that `graphs.graph` must point at the path inside the container
where the properties file is mounted. The full Gremlin Server
reference is at
<https://tinkerpop.apache.org/docs/current/reference/#_configuring_2>.
