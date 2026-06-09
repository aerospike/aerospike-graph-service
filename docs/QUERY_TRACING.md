# Query tracing

Aerospike Graph Service can emit OpenTelemetry spans to a Zipkin
collector for slow-query analysis.

Official overview: [Query tracing](https://aerospike.com/docs/graph/observe/query-tracing).

## Quickstart

Stand up a local Zipkin:

```bash
docker run -d -p 9411:9411 openzipkin/zipkin
```

When Aerospike Graph Service runs in the host JVM alongside Zipkin in
Docker, the defaults (below) will work out of the box. When the graph
service itself runs in a container, set
`aerospike.graph.query-tracing.opentelemetry-host` to the IP of the
Zipkin container (on Linux bridged Docker, `172.17.0.1`). Open the
Zipkin UI at <http://localhost:9411> to browse traced queries.

## Configuration

`aerospike.graph.script-logging.redact-literals`

* Redacts literals for all things that log or export query scripts
* Default: false

`aerospike.graph.query-tracing.sampling-percentage`

* Percentage between 1-100 of queries that exceed the `aerospike.graph.query-tracing.threshold-ms` that will be traced
* Default: 100

`aerospike.graph.query-tracing.threshold-ms`

* Threshold in milliseconds when exceeded the query will be traced
* Default: -1 (disabled, any number >= 0 will enable).

`aerospike.graph.query-tracing.opentelemetry-host`

* Host IP for Zipkin
* Default: localhost (for linux using 172.17.0.1 for adjacent docker container works)

`aerospike.graph.query-tracing.opentelemetry-port`

* Port for Zipkin
* Default: 9411
