# Prometheus Support
Aerospike Graph supports exporting metrics via prometheus.

## Enabling the Prometheus Exporter
The Prometheus exporter can be enabled/disabled by exposing the port in docker.

## Configuring the Prometheus Exporter

The prometheus exporter runs on port `9090` by default a default path of `/metrics`.

These can be overridden by setting the following environment variables or config options in the properties file:

```
aerospike.graph.http.port=1234
aerospike.graph.prometheus.path=/custom_path
```

Note: if running in docker, port remapping could also be used to switch the default port in the container to another port externally.

## Metrics Available

#### GremlinServer Metrics

The metrics available through GremlinServer are listed in the 
[TinkerPop documentation](https://tinkerpop.apache.org/docs/current/reference/#metrics).

#### JVM Metrics

JVM metrics related to garbage collection, process info, buffers, memory, classes, and threads are available.