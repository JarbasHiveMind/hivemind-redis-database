# Redis Cluster Consistency Plan

## Current State

This backend stores one logical client across multiple Redis keys:

- `client:client:<id>`
- `client:name:<name>`
- `client:api_key:<api_key>`
- `client:idx:<id>`
- `client:count`
- `client:id_seq`

In Redis Cluster, those keys map to different hash slots by default. Legacy
cluster mode therefore works, but multi-key writes are not atomic. A failure in
the middle of `add_item()`, `update_client()`, or `remove_client()` can leave
indexes or counters temporarily inconsistent.

The backend now has `sync()` to repair drift, but `sync()` is a recovery tool,
not a transaction boundary.

The backend now also supports an optional `cluster_hash_tag` setting. When
enabled, all keys for that namespace are stored in the same Redis Cluster slot
and writes use `RedisCluster.pipeline(transaction=True)`.

## Smallest Safe Production Migration

The safest next step is not a broad redesign. It is a targeted schema update
for cluster deployments:

1. Enable a fixed cluster hash tag, for example `cluster_hash_tag="clients"`.
2. Generate all Redis keys for that deployment under the same slot, for example:
   - `client:{clients}:client:1`
   - `client:{clients}:name:alpha`
   - `client:{clients}:api_key:alpha-key`
   - `client:{clients}:idx:1`
   - `client:{clients}:count`
   - `client:{clients}:id_seq`
3. In cluster mode with that tag enabled, the backend uses `RedisCluster.pipeline(transaction=True)`.

Because every key shares the same hash tag, all commands map to the same slot.
That is the minimum change that allows real Redis Cluster transactions for this
schema.

## Why This Is The Right Tradeoff

- The client database is small and metadata-heavy. It does not benefit much from
  distributing individual index keys across shards.
- A single-slot namespace keeps the existing schema and query model.
- The application gets actual atomic updates in cluster mode without inventing a
  more complex write protocol.
- Recovery and rollback stay simple.

## Recommended Rollout

1. Keep the legacy key layout as the default for backward compatibility.
2. Migrate existing cluster deployments during a maintenance window:
   - stop writers
   - snapshot the Redis namespace
   - scan legacy `client:client:*` records
   - re-write them through the new tagged backend into the new namespace
   - run `sync()` on the new namespace
   - validate `len(db)`, `search_by_value("name", ...)`, and `search_by_value("api_key", ...)`
   - switch production config to the tagged namespace
3. Keep the legacy namespace for rollback until the new deployment is stable.

## What Not To Do

- Do not try to fake atomicity across cluster slots with ordinary pipelines.
- Do not use distributed locks here unless there is a much stronger consistency
  requirement; that adds operational complexity without solving the schema issue.
- Do not switch the default key format in place. That would silently orphan
  existing data.

## Practical Recommendation

For new cluster deployments, enable `cluster_hash_tag` from the start.
For existing cluster deployments, migrate deliberately and keep `sync()` as the
repair path during rollout.
