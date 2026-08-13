# Metadata Set Config Call

Use `aerospike.graph.admin.metadata.set-config` to update supported runtime configuration keys without restarting AGS.

## Call Name

`aerospike.graph.admin.metadata.set-config`

## Gremlin Usage

At least one key/value pair is required.

```gremlin
g.call("aerospike.graph.admin.metadata.set-config")
 .with("aerospike.client.policy.write.socketTimeout", "10000")
 .with("aerospike.client.policy.write.totalTimeout", "3000")
 .next()
```

Example response:

```text
Successfully updated configuration {aerospike.client.policy.write.totaltimeout=3000, aerospike.client.policy.write.sockettimeout=10000}.
```

Configuration keys are matched case-insensitively (they are normalized
to lowercase internally), but the canonical camelCase forms shown above
are recommended.

## HTTP Usage

Endpoint:

`/<graphId>/admin/metadata/set-config`

Example:

```text
GET /myGraph/admin/metadata/set-config?aerospike.client.policy.write.socketTimeout=10000&aerospike.client.policy.write.totalTimeout=3000
```

## Supported Runtime Key Patterns

Only mutable keys can be changed at runtime:

- `aerospike.client.policy.*`
- `aerospike.client.batch.read.*`
- `aerospike.graph.pagination.*`
- `aerospike.client.scan.max.wait`
- `aerospike.client.batch-threshold.per-node`
- `aerospike.graph.movement.barrier.size`
- `aerospike.client.infoPolicy.timeout`
- `aerospike.graph.cache.*`

## Validation and Errors

- Empty parameter map is rejected.
- Updating immutable keys fails with: `Immutable option <key> can't be changed.`
- If authentication is enabled, the caller must have the `ADMIN` role.

## Persistence and Multi-Instance Behavior

- **Preserved across restart:** Yes. Updates are saved in graph metadata and applied again when AGS restarts.
- **Shared across server instances:** Yes. Instances using the same graph metadata read the same saved config.
- **Propagation timing:** Other running instances pick up changes on their next config refresh cycle (or immediately after explicit refresh/restart), not necessarily instantly.
  - If `aerospike.graph.config.update.enabled=true`, the approximate propagation time is one refresh interval.
  - The interval is controlled by `aerospike.graph.config.update.frequency` (default: `5000` ms, about 5 seconds).
  - If `aerospike.graph.config.update.enabled=false` (default), instances will not auto-refresh and typically need restart to pick up changes.
