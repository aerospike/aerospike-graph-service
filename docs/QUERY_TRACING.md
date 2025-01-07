# Query Tracing

Reference for quick start and things that will need to be documented.

### Quickstart

To set up Zipkin to enable query tracing:

```
docker run -d -p 9411:9411 openzipkin/zipkin
```

Default configurations will work for Firefly in JVM and Zipkin in Docker. If running in an AGS instance change the host 
to the IP of the Zipkin docker image. Can access the Zipkin UI to see slow query logs at localhost:9411.

### Configurations

#TODO:
<send video recording of this feature to prd-graph for feedback | explicitly ask Zohar if he doesn't look>

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
