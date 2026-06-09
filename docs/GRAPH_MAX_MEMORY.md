# Setting the max memory of the Docker container

Container memory limits and JVM heap size are not the same. Increasing the
container limit does not increase the heap unless you configure the JVM to
use it.

By default, AGS uses `-XX:MaxRAMPercentage=80.0`, so the JVM heap can use up
to 80% of the container memory limit. The JVM reads cgroup limits in Docker,
Kubernetes, Fargate, and similar environments.

## Recommended: configure heap through AGS properties

Set `aerospike.graph-service.heap.max` (and optionally
`aerospike.graph-service.heap.min`) in your properties file or as
environment variables. See [`CONFIG_OPTIONS.md`](CONFIG_OPTIONS.md) and the
[Configuration reference](https://aerospike.com/docs/graph/reference/config).

Example properties file:

```properties
aerospike.graph-service.heap.max=32g
```

Example Docker run with a properties file mount:

```bash
docker run -p 8182:8182 -p 9090:9090 \
  -v $PWD/aerospike-graph.properties:/opt/aerospike-graph/aerospike-graph.properties \
  -e aerospike.client.host=HOSTNAME:PORT \
  -e aerospike.client.namespace=NAMESPACE \
  aerospike/aerospike-graph-service:latest
```

Replace `HOSTNAME:PORT` and `NAMESPACE` using [Connection values](../README.md#connection-values) in the README.

## Alternative: `JAVA_OPTIONS`

You can pass `-Xmx` through the `JAVA_OPTIONS` environment variable when you
need to set heap size without a properties file, or to override a
properties-file setting at runtime:

```bash
docker run -p 8182:8182 -p 9090:9090 \
  -e JAVA_OPTIONS="-Xmx32768m" \
  -e aerospike.client.host=HOSTNAME:PORT \
  -e aerospike.client.namespace=NAMESPACE \
  aerospike/aerospike-graph-service:latest
```

Prefer `aerospike.graph-service.heap.max` when possible so heap settings
stay with other AGS configuration.
