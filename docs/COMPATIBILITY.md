# Compatibility

This page pins down the supported versions of Aerospike Graph
Service against everything it talks to. It is kept in lock-step with
the root `pom.xml` and with release-note additions.

## At a glance (current `3.x-dev`)

| Component                           | Supported                             | Notes                                               |
|-------------------------------------|---------------------------------------|-----------------------------------------------------|
| **JDK (runtime + build)**           | 11, 17                                | 11 is the source/target. 17 works as the runtime.   |
| **Apache TinkerPop / Gremlin**      | 3.7.3                                 | Server + driver must match the same 3.7.x line.     |
| **Aerospike Database (server)**     | 7.0+                                  | Minimum supported version. MRT and related APIs build on capabilities available from Aerospike Database 6.0 onward. |
| **Aerospike Java client**           | 9.3.x                                 | Wire protocol: Aerospike 7+ server.                 |
| **Spark (OLAP + bulk loader)**      | 3.5.x                                 | Both Spark side processes are tested on 3.5.        |
| **Docker image base**               | `eclipse-temurin:17-jre-jammy`        | Produced by the `publish-ghcr-container.yml` flow.  |
| **Maven (build)**                   | 3.9+                                  | Enforced by `.mvn/maven-version` / CI.              |

The authoritative versions are the properties at the top of the root
[`pom.xml`](../pom.xml) (`<aerospike-client.version>`,
`<tinkerpop.version>`, `<java.version>`). If this table and the POM
ever drift, **the POM wins**. Open a PR to correct this
document.

## Aerospike server features relied on

The graph service is not a thin wrapper over basic kv. It uses:

- **Secondary indexes** on list / map sub-paths, used by index-driven
  traversals (`has(...)` with a value filter).
- **Batch operations** with per-key operation lists, used on every
  multi-key read and write.
- **Multi-record transactions (MRT)** when
  `aerospike.graph.transaction.enabled=true`: requires Aerospike
  server 7.0+.
- **Query filters / `QueryPolicy.filterExp`**: server-side filter
  expressions pushed down from Gremlin `has(...)` steps.
- **Set-level truncate**: used by admin operations and tests.
- **CDT** (list and map ops) as the carrier format for vertex and edge
  properties.

Running against a pre-7.0 server is not supported and not tested.
Some features (MRT, specific CDT ops) will return errors the service
does not currently translate to user-friendly messages.

## TinkerPop / Gremlin

The service implements the full
[Apache TinkerPop Graph API](https://tinkerpop.apache.org/docs/3.7.3/reference/)
for the pinned 3.7.x line. That means a Gremlin driver pinned to a
**matching 3.7.x minor** will work out of the box. A 3.6 client, a
3.8 client, or a 4.x preview will likely not: bytecode, serializer,
and strategy contracts change between minor lines, and TinkerPop 3.8
in particular introduced breaking changes that we have not yet
adopted. Tracking 3.8 / 4.x is on the roadmap but not scheduled.

Compatibility is verified on every PR via the standard TinkerPop
`ProcessStandardTest` / `StructureStandardTest` harnesses: see the
`Firefly*ProcessStandardTest` and `Firefly*StructureStandardTest`
classes.

## JDK

- `java.version` in the POM is `11`: that is the `--source` /
  `--target` pair. Building with JDK 11 produces class files that run
  on JDK 11+.
- Production Docker images use JDK 17 by default (Temurin jammy).
- The test matrix covers JDK 11 and JDK 17. JDK 21 is on the roadmap
  but not yet in CI.

## Aerospike Java client

Pinned to `9.3.x`. The service depends on behavior only available in
the 9.x line (batch-write policy shape, MRT client API,
`filterExp` placement). Downgrading to an 8.x client will fail to
compile.

## Spark (OLAP + bulk loader)

Both side processes (`aerospike-graph-olap` / `GraphComputer` and
`aerospike-graph-bulk-loader`) are tested on Spark 3.5.x. They
should also work on any 3.4+ release that has the same Kryo
serializer defaults, but that is not part of the CI matrix; file an
issue before relying on a different line.

## Upgrade policy

- **Patch** (`x.y.z` → `x.y.z+1`): no behavior or wire-format changes
  expected. Safe to drop in.
- **Minor** (`x.y` → `x.y+1`): additive behavior only. Existing
  configs and data remain valid. Release notes may tighten guarantees
  (e.g. new validator for a pre-existing config key).
- **Major** (`x` → `x+1`): reserved for breaking changes. Accompanied by
  release notes with step-by-step upgrade guidance.

## Reporting an incompatibility

If something in this matrix is wrong or stale, open an issue (or a PR
against this file) with:

- Aerospike server build string (`asinfo -v build`).
- JDK version (`java -version`).
- Gremlin driver coordinates (`org.apache.tinkerpop:gremlin-driver:…`).
- The failing query / operation.

Compatibility-matrix bugs are a standing priority and the fastest
class of issue to get merged.
