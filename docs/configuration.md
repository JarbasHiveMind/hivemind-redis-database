# Configuration Reference

Pass all settings in the `hivemind-redis-db-plugin` block of
`~/.config/hivemind-core/server.json`.

| Setting | Default | Description |
|---|---|---|
| `name` | `"clients"` | HiveMind-core database contract field. |
| `subfolder` | `"hivemind-core"` | HiveMind-core database contract field. Not used for Redis key naming. |
| `host` | `"127.0.0.1"` | Redis host for single-node mode. |
| `port` | `6379` | Redis port. |
| `db` | `0` | Redis DB number (single-node only. Ignored in cluster mode). |
| `username` | `"default"` | Redis ACL username. |
| `password` | none | Redis password. |
| `index_prefix` | `"client"` | Key namespace prefix used inside Redis. |
| `cluster_nodes` | none | List of `{"host": ..., "port": ...}` dicts for Redis Cluster startup. Presence triggers cluster mode. |
| `cluster_hash_tag` | none | Fixed hash tag for one-slot transactional writes. Recommended for new cluster deployments. |
| `max_connections` | `5` | Redis connection pool size. |
| `retry_attempts` | `3` | Internal retry count for transient operations. |
| `retry_delay` | `0.1` | Seconds between retry attempts. |
| `use_ssl` | `false` | Enable TLS. |
| `ssl` | none | Backward-compatible alias for `use_ssl`. Prefer `use_ssl` in new configs. |
| `ssl_certfile` | none | Path to client certificate (mTLS). |
| `ssl_keyfile` | none | Path to client key (mTLS). |
| `ssl_ca_certs` | none | Path to CA bundle. |
| `ssl_cert_reqs` | none | TLS verification mode: `"required"`, `"optional"`, or `"none"`. |
| `ssl_check_hostname` | `true` | Hostname validation. Forced off when `ssl_cert_reqs="none"`. |

## Single Redis

```json
{
  "database": {
    "module": "hivemind-redis-db-plugin",
    "hivemind-redis-db-plugin": {
      "name": "clients",
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

## Redis Cluster (recommended mode)

```json
{
  "database": {
    "module": "hivemind-redis-db-plugin",
    "hivemind-redis-db-plugin": {
      "name": "clients",
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

Omit `cluster_hash_tag` to keep the legacy untagged key layout during a
transitional period. See [cluster_consistency.md](cluster_consistency.md).

## TLS

```json
{
  "database": {
    "module": "hivemind-redis-db-plugin",
    "hivemind-redis-db-plugin": {
      "name": "clients",
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

---
[← Architecture](architecture.md) · [Home](../README.md) · [Cluster Consistency →](cluster_consistency.md)
