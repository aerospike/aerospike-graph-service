# Prometheus Support
Aerospike Graph supports exporting metrics via prometheus.

## Enabling the Prometheus Exporter
The Prometheus exporter is enabled by default through a plugin in the gremlin-server.yaml file.

## Disabling the Prometheus Exporter
To disable the prometheus exported, you must supply the docker container with a customer gremlin-server.yaml file,
which we have separate documentation for.

The custom gremlin-server.yaml file must remove the following line:
```
      com.aerospike.firefly.jsr223.FireflyGremlinPlugin: {},
```
from the section
```
scriptEngines: {
  gremlin-groovy: {
    plugins: {
      com.aerospike.firefly.jsr223.FireflyGremlinPlugin: {},
      org.apache.tinkerpop.gremlin.server.jsr223.GremlinServerGremlinPlugin: {},
      org.apache.tinkerpop.gremlin.tinkergraph.jsr223.TinkerGraphGremlinPlugin: {},
      org.apache.tinkerpop.gremlin.jsr223.ImportGremlinPlugin: {classImports: [java.lang.Math], methodImports: [java.lang.Math#*]},
      org.apache.tinkerpop.gremlin.jsr223.ScriptFileGremlinPlugin: {files: [scripts/empty-sample.groovy]}}
  }}
```

Doing this will remove the plugin and the prometheus exporter will not start.

## Configuring the Prometheus Exporter

The prometheus exporter runs on port `9090` by default a default path of `/metrics`.

These can be overridden by setting the following environment variables or config options in the properties file:

```
aerospike.graph.prometheus.port=1234
aerospike.graph.prometheus.path=/custom_path
```

Note: if running in docker, port remapping could also be used to switch the default port in the container to another port externally.

## Metrics Available

#### GremlinServer Metrics

The metrics available through GremlinServer are listed in the 
[TinkerPop documentation](https://tinkerpop.apache.org/docs/current/reference/#metrics).

#### JVM Metrics

JVM metrics related to garbage collection, process info, buffers, memory, classes, and threads are available.