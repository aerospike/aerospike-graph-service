# Compatibility

This page records the current Aerospike Graph Service compatibility
baseline and optional feature requirements.

## Current baseline

| Component                  | Version or Boundary           | Notes                                     |
| -------------------------- | ----------------------------- | ----------------------------------------- |
| Java Build Target          | Java 11                       | Compiles source and target 11 bytecode.   |
| Container Runtime          | JDK 17                        | Used by the Docker images.                |
| Apache TinkerPop / Gremlin | 3.7.3                         | Drivers should use the 3.7.x line.        |
| Aerospike Java Client      | 10.3.0                        |                                           |
| Spark Dependencies         | 3.5.8, Scala 2.12             | Used by the bulk loader and OLAP modules. |
| Container Base             | Alpine 3.24.1 with OpenJDK 17 | Used by the full and slim Docker images.  |

## Aerospike Database

Aerospike Graph Service requires Aerospike Database `6.2.0.7` or
newer. Community Edition works without a feature key. If you choose
Enterprise or Standard Edition, provide the feature key required by
that database edition.

Some features have stricter requirements:

- Multi-record transactions require Aerospike Database 8+ with strong
  consistency and are enabled with `aerospike.graph.mrt.enabled`.
- TinkerPop transaction support is configured separately with
  `aerospike.graph.tx.enabled`.
- Expression and compound indexes require Aerospike Database 8.1+.

## Optional AGS Features

This table lists only optional AGS capabilities with requirements above
the `6.2.0.7` baseline or that require an Enterprise Edition
capability. In the Enterprise column, `☑` means the feature requires
Enterprise Edition; `☐` means it is available in Community Edition.

| Feature                       | AGS Configuration or API                             | Minimum Database Version | Enterprise Required |
| ----------------------------- | ---------------------------------------------------- | ------------------------ | ------------------- |
| Expression / Compound Indexes | Compound-index configuration and admin calls         | 8.1.0                    | ☐ No                |
| TinkerPop Transactions        | `aerospike.graph.tx.enabled` and `graph.tx()`        | 8.0.0 with SC namespace  | ☑ Yes               |
| Multi-Record Transactions     | `aerospike.graph.mrt.enabled`                        | 8.0.0 with SC namespace  | ☑ Yes               |
| Database RBAC                 | `aerospike.client.user`, `password`, and `auth.mode` | 6.2.0.7                  | ☑ Yes               |

Community Edition works without a feature key. Enterprise and Standard
Edition deployments require the feature key appropriate to that
database edition.

XDR is not an AGS requirement. If configured independently of AGS,
refer to Aerospike Database documentation for its edition and version
requirements.

## TinkerPop / Gremlin

AGS targets TinkerPop 3.7.3. It has documented TinkerPop opt-outs, so
it does not claim complete implementation of every TinkerPop Graph API
feature.

## Spark

The bulk loader uses Spark 3.5.8. The OLAP module compiles against the
same Spark version. Other Spark versions are not documented as
compatible.

## Reporting an incompatibility

Include the Aerospike build string, JDK version, Gremlin driver
coordinates, deployment configuration, and failing operation when
opening an issue.
