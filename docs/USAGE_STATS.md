# Usage stats

> **TL;DR — nothing is reported externally.** Aerospike Graph Service
> does not send telemetry, crash reports, or any other data off-box.
> The "usage stats" described here are a self-instrumentation feature:
> the service periodically writes its own resource-usage numbers
> (vCPU count, memory size, epoch timestamps) into a metadata set in
> the same Aerospike namespace it is serving graph data out of. You
> query those stats yourself, from your own cluster, with a Gremlin
> call. There is no external endpoint involved.

## What is collected

Each running instance of the service writes, at startup and periodically
thereafter, a record to a metadata set inside your Aerospike cluster
containing:

| Field            | Value                                                          |
|------------------|----------------------------------------------------------------|
| `uuid`           | Random UUID generated at process start (per-instance)          |
| `epoch-ms-start` | Wall-clock time the instance started                           |
| `epoch-ms-final` | Wall-clock time of the most recent ticker update               |
| `vcpus`          | Number of vCPUs the JVM reports as available                   |
| `memory-gb`      | JVM heap size in gigabytes                                     |

Nothing else. No query content, no schema, no IP addresses, no
user-identifying information, no hostnames.

## Where it is stored

In the same Aerospike namespace configured via
`aerospike.client.namespace`, under a metadata set that the graph
service uses for its own bookkeeping. The records live and die with
your cluster. If you drop the namespace, they are gone.

## Querying the stats

The stats are surfaced through a Gremlin call step. Across all
instances that have ever written to this namespace:

```
g.call("aerospike.graph.metadata.usage").next()
```

Returns a `Map<String, Object>` with:

- `raw` — the raw per-instance stats records.
- `total-vcpu` — aggregated vCPU-years across all instances.

Filtering by start date:

```
g.call("aerospike.graph.metadata.usage").with("since", "2023-03-30").next()
```

Returns the same shape, but aggregated only over instances whose
`epoch-ms-start` is on or after the `since` date.

Example output:

```
{   'raw': [   {   'epoch-ms-final': 1702327851857,
                   'epoch-ms-start': 1702326901857,
                   'memory-gb': 32,
                   'uuid': '9c0ba414-c9f2-492f-adb9-9792bb6dfed1',
                   'vcpus': 16},
               {   'epoch-ms-final': 1702327148084,
                   'epoch-ms-start': 1702327148084,
                   'memory-gb': 9,
                   'uuid': 'c924f45e-886d-4c04-8039-292d56ea037c',
                   'vcpus': 16}],
    'total-vcpu': 0.0004819888381532217 }
```

## Can I disable the writer?

Yes. Set the following configuration value:

```
aerospike.graph.usage.enabled=false
```

When this is `false` the background timer is never scheduled and no
records are written to the usage-stats set. The default is `true` so
existing deployments keep working unchanged. Since the writer only
touches your own cluster, it cannot be used for external telemetry
regardless of how this flag is set.

If you also want to remove previously-written records, drop the
metadata set once: the service will not re-create it until the flag
is flipped back on.
