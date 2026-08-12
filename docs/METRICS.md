# Prometheus Support

Aerospike Graph Service supports exporting metrics through Prometheus.

## Enabling the Prometheus Exporter

The metrics endpoint is served by the built-in HTTP server, which is
controlled by `aerospike.graph.http.enabled` (default `true`). When it
is enabled, set `aerospike.graph.http.port` (default `9090`) in the
properties file or as an environment variable, then publish that port
when you run the container (for example `-p 9090:9090`).

## Configuring the Prometheus Exporter

The prometheus exporter runs on port `9090` by default with a default path of `/metrics`.

These can be overridden by setting the following environment variables or config options in the properties file:

```
aerospike.graph.http.port=1234
aerospike.graph.prometheus.path=/custom_path
```

Note: if running in docker, port remapping could also be used to switch the default port in the container to another port externally.

By default (`aerospike.graph.prometheus.rename.enabled=true`), some
exported metric names are rewritten to AGS-friendly names. Set it to
`false` to keep the raw metric names.

The same HTTP server also exposes a health-check endpoint at
`/healthcheck` (and `/<graphId>/healthcheck`), returning `200` when the
service is healthy and `503` otherwise.

## Metrics Available

#### GremlinServer Metrics

The metrics available through GremlinServer are listed in the 
[TinkerPop documentation](https://tinkerpop.apache.org/docs/current/reference/#metrics).

#### JVM Metrics

JVM metrics related to garbage collection, process info, buffers, memory, classes, and threads are available.

#### AGS Metrics

AGS also exports a small number of service-specific metrics:

- `usage`: Aerospike Graph Service usage in vCPU-hours.
- `cluster_name`: the connected Aerospike cluster name.