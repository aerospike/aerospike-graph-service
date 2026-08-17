# Running Aerospike Graph Service in Docker

User guides and deployment procedures: [aerospike.com/docs/graph](https://aerospike.com/docs/graph).

[Apache TinkerPop®][tinkerpop]-compatible graph database backed by
[Aerospike][aerospike].

<img src="https://raw.githubusercontent.com/apache/tinkerpop/master/docs/static/images/tinkerpop-character.png" alt="TinkerPop" width="100" />

> Released user-facing images are published on Docker Hub as
> `aerospike/aerospike-graph-service:VERSION` (with a moving `:latest`
> tag). The product name is Aerospike Graph Service.

[tinkerpop]: http://tinkerpop.apache.org
[aerospike]: https://aerospike.com

## Prerequisites

Before you run AGS in Docker, you need:

- An Aerospike Database deployment compatible with this release. AGS works with
  Community Edition; Enterprise and Standard Edition deployments require the
  feature key appropriate to that database edition.
- An Aerospike Database version compatible with this release. See [`COMPATIBILITY.md`](COMPATIBILITY.md).
- A namespace that already exists on the cluster with the [`default-ttl`](https://aerospike.com/docs/database/reference/config#namespace__default-ttl) configuration option set to `0`. See [TTL on the Aerospike namespace](https://aerospike.com/docs/graph/deploy/docker#ttl-on-the-aerospike-namespace).

## Connection values

`HOSTNAME:PORT` and `NAMESPACE` come from your Aerospike cluster:

- **Seed address (`HOSTNAME:PORT`)**: Aerospike seed hostname or IP plus service port (default `3000`). When AGS and Aerospike both run in Docker on one host, use the Aerospike container IP or see [Deploy Aerospike Graph Service with Docker](https://aerospike.com/docs/graph/deploy/docker#prerequisites).
- **Namespace (`NAMESPACE`)**: existing namespace name from `aerospike.conf` or `show namespaces` in [`asadm`](https://aerospike.com/docs/database/tools/asadm). The Aerospike [Install with Docker](https://aerospike.com/docs/database/install/docker/) quick start uses `test`.

## 1. Quickstart with environment variables

The simplest way to start the service. Suitable for demos and local
development. Not recommended for production because only a subset of
configuration is reachable through environment variables.

Use your cluster values in place of the placeholders below:

```bash
docker run -d --name graph \
  -p 8182:8182 \
  -e aerospike.client.host="HOSTNAME:PORT" \
  -e aerospike.client.namespace="NAMESPACE" \
  -e aerospike.graph.index.vertex.properties=property1,property2 \
  -e aerospike.graph.index.vertex.label.enabled=true \
  aerospike/aerospike-graph-service:latest
```

Environment variable names must start with `aerospike` and use the same keys as in a properties file.

## 2. Properties file (recommended for most deployments)

Mount a properties file at the path the container entrypoint reads:

```bash
docker run -d --name graph \
  -p 8182:8182 \
  -v /host/path/aerospike-graph.properties:/opt/aerospike-graph/aerospike-graph.properties \
  aerospike/aerospike-graph-service:latest
```

Example `aerospike-graph.properties`:

```properties
gremlin.graph=com.aerospike.firefly.structure.FireflyGraph
aerospike.client.host=HOSTNAME:PORT
aerospike.client.namespace=NAMESPACE
aerospike.graph.index.vertex.label.enabled=true
aerospike.graph.index.vertex.properties=property1,property2
```

See [`CONFIG_OPTIONS.md`](CONFIG_OPTIONS.md) for every tunable and the [Configuration reference](https://aerospike.com/docs/graph/reference/config) on aerospike.com.

## 3. Custom Gremlin Server YAML (advanced)

For deployments that need to override Gremlin Server internals
(serializers, timeouts, metrics reporters, and similar settings), mount
a custom YAML at `/opt/aerospike-graph/custom/aerospike-graph-service.yaml`.
The container entrypoint uses that file when it exists.

You are responsible for linking the YAML to a properties file. You can
still set heap options through `aerospike.graph-service.heap.*`
environment variables or properties.

```bash
docker run -d --name graph \
  -p 8182:8182 \
  -v /host/path/aerospike-graph.properties:/opt/aerospike-graph/aerospike-graph.properties \
  -v /host/path/aerospike-graph-service.yaml:/opt/aerospike-graph/custom/aerospike-graph-service.yaml \
  aerospike/aerospike-graph-service:latest
```

See [`SETUP.md`](SETUP.md) for the same path documented outside Docker.
The full Gremlin Server reference is at
<https://tinkerpop.apache.org/docs/current/reference/#_configuring_2>.
