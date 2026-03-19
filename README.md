# HiveMind Redis Database

Redis-backed client database plugin for HiveMind-core.

It supports:

- single Redis
- Redis Cluster
- optional TLS
- optional RediSearch / Redis Stack acceleration
- transactional same-slot writes in cluster mode via `cluster_hash_tag`
- repair and migration tooling for production rollouts

## Why This Backend

This backend keeps HiveMind client records in Redis with secondary indexes for
`name` and `api_key`.

Key operational points:

- it auto-detects single Redis vs Redis Cluster when you point `host` and `port`
  at a live node
- it uses RediSearch when the module is available, and falls back to normal
  Redis indexing when it is not
- it keeps exact-match search semantics even when RediSearch is enabled
- it hides revoked clients from iteration and search results
- it exposes `sync()` to rebuild counters, set indexes, and search hashes after
  interrupted writes or manual Redis changes

## Installation

```bash
pip install hivemind-redis-database
```

Compatibility:

- Python 3.10+
- Redis for single-instance mode
- Redis Cluster for cluster mode
- Redis Stack or Redis with the RediSearch module loaded for advanced search

## Deployment Modes

| Mode | Use when | Notes |
| --- | --- | --- |
| Single Redis | You want the simplest production setup | Supports `db`, TLS, RediSearch, and normal Redis indexing |
| Redis Cluster, legacy mode | You already have an existing cluster namespace | Compatible with current key layout, but multi-key writes are best-effort across slots |
| Redis Cluster with `cluster_hash_tag` | New cluster deployments | Recommended mode; keeps all keys for one namespace in one slot and enables transactional cluster pipelines |
| Redis Stack / RediSearch | You want faster indexed search on `name` and `api_key` | Optional; fallback indexing still works without it |

Recommendation:

- new Redis Cluster deployments: enable `cluster_hash_tag` from day one
- existing Redis Cluster deployments: migrate first, then enable `cluster_hash_tag`
- single-node Redis deployments: no migration concerns, just use the normal config

## HiveMind-core Configuration

HiveMind-core loads this plugin from `server.json` and passes the plugin config
through directly.

Important note:

- `name` and `subfolder` are part of the HiveMind-core database contract
- `index_prefix` controls the actual Redis key namespace used by this backend

### Single Redis

```json
{
  "database": {
    "module": "hivemind-redis-db-plugin",
    "hivemind-redis-db-plugin": {
      "name": "clients",
      "subfolder": "hivemind-core",
      "host": "127.0.0.1",
      "port": 6379,
      "db": 1,
      "password": "",
      "index_prefix": "client",
      "max_connections": 10
    }
  }
}
```

### Redis Cluster, Recommended Mode

```json
{
  "database": {
    "module": "hivemind-redis-db-plugin",
    "hivemind-redis-db-plugin": {
      "name": "clients",
      "subfolder": "hivemind-core",
      "cluster_nodes": [
        {"host": "redis-node1", "port": 6379},
        {"host": "redis-node2", "port": 6379},
        {"host": "redis-node3", "port": 6379}
      ],
      "cluster_hash_tag": "clients",
      "password": "your_password",
      "index_prefix": "client",
      "max_connections": 20
    }
  }
}
```

If you are staying on legacy cluster mode temporarily, omit
`cluster_hash_tag`.

### TLS / SSL

```json
{
  "database": {
    "module": "hivemind-redis-db-plugin",
    "hivemind-redis-db-plugin": {
      "name": "clients",
      "subfolder": "hivemind-core",
      "host": "redis.example.com",
      "port": 6380,
      "use_ssl": true,
      "ssl_certfile": "/path/to/client.crt",
      "ssl_keyfile": "/path/to/client.key",
      "ssl_ca_certs": "/path/to/ca.crt",
      "ssl_cert_reqs": "required",
      "ssl_check_hostname": true
    }
  }
}
```

`ssl` is still accepted as a backward-compatible alias, but `use_ssl` is the
preferred config key in new docs and new deployments.

## Configuration Reference

| Setting | Meaning | Default / Notes |
| --- | --- | --- |
| `name` | HiveMind-core database contract field | Default: `"clients"` |
| `subfolder` | HiveMind-core database contract field | Default: `"hivemind-core"` |
| `host` | Redis host | Default: `"127.0.0.1"` |
| `port` | Redis port | Default: `6379` |
| `db` | Redis DB number for single-node mode | Default: `0`; ignored in cluster mode |
| `username` | Redis ACL username | Default: `"default"` |
| `password` | Redis password | Optional |
| `index_prefix` | Actual Redis namespace prefix used by this backend | Default: `"client"` |
| `cluster_nodes` | Explicit Redis Cluster startup nodes | Accepts the documented `[{\"host\": ..., \"port\": ...}]` shape |
| `cluster_hash_tag` | Fixed hash tag for one-slot transactional writes in cluster mode | Recommended for new cluster deployments |
| `max_connections` | Redis connection pool size | Default: `5` |
| `retry_attempts` | Internal retry attempts for transient operations | Default: `3` |
| `retry_delay` | Delay between retry attempts | Default: `0.1` seconds |
| `use_ssl` | Enable TLS | Default: `false` |
| `ssl` | Backward-compatible alias for `use_ssl` | Prefer `use_ssl` in new configs |
| `ssl_certfile` | Client certificate path | Optional |
| `ssl_keyfile` | Client key path | Optional |
| `ssl_ca_certs` | CA bundle path | Optional |
| `ssl_cert_reqs` | TLS verification mode | `"required"`, `"optional"`, or `"none"` |
| `ssl_check_hostname` | Hostname validation for TLS | Default: `true`; forced off when `ssl_cert_reqs="none"` |

## Runtime Behavior

- `search_by_value("name", value)` and `search_by_value("api_key", value)` use
  RediSearch when available.
- If RediSearch is not available, the backend falls back to Redis set indexes.
- Search remains exact-match. RediSearch is used as an accelerator, not as fuzzy
  search.
- Revoked clients are excluded from iteration and search results so HiveMind
  admin flows only see active clients.
- `sync()` repairs drift by rebuilding counters, Redis set indexes, and
  RediSearch hash documents from stored client records.

## Python Usage

```python
from hivemind_plugin_manager.database import Client
from hivemind_redis_database import RedisDB

db = RedisDB(host="127.0.0.1", port=6379)

assert db.health_check()

client = Client(client_id=1, api_key="alpha-key", name="alpha")
db.add_item(client)

matches = db.search_by_value("name", "alpha")
print([c.name for c in matches])

client.name = "beta"
client.api_key = "beta-key"
db.update_client(client)

db.remove_client(str(client.client_id))
db.sync()
```

Cluster example:

```python
from hivemind_redis_database import RedisDB

db = RedisDB(
    cluster_nodes=[
        {"host": "redis-node1", "port": 6379},
        {"host": "redis-node2", "port": 6379},
    ],
    cluster_hash_tag="clients",
    password="your_password",
)
```

## Migrating Existing Redis Cluster Deployments

Do not enable `cluster_hash_tag` in place on an existing cluster namespace.
That changes the Redis key layout.

Safe rollout:

1. Stop HiveMind writers for a short maintenance window.
2. Back up the current Redis namespace.
3. Run a dry run to confirm the source and target namespaces.
4. Run the migration tool to copy records into the tagged namespace.
5. Update `server.json` to set `cluster_hash_tag`.
6. Restart HiveMind and smoke-test add, get, update, revoke, and search flows.
7. Keep the legacy namespace for rollback until the new deployment is stable.

Dry run:

```bash
hivemind-redis-migrate-cluster \
  --config ~/.config/hivemind-core/server.json \
  --target-cluster-hash-tag clients \
  --dry-run
```

Migration:

```bash
hivemind-redis-migrate-cluster \
  --config ~/.config/hivemind-core/server.json \
  --target-cluster-hash-tag clients \
  --clear-target
```

Notes:

- `--clear-target` clears keys in the target namespace before copying
- `--source-cluster-hash-tag` is available if you are migrating from one tagged
  namespace to another
- the migration tool copies raw client records and then runs `sync()` on the
  target namespace

More detail is in [docs/cluster_consistency.md](docs/cluster_consistency.md).

## Development

Useful local checks:

```bash
python -m unittest discover -s tests -v
python -m build --wheel
```

CI covers:

- unit tests
- single Redis Stack integration
- TLS Redis integration
- Redis Cluster integration
- Redis Stack Cluster integration
