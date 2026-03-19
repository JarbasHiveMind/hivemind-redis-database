# HiveMind Redis Database

Redis database plugin for HiveMind with consistent indexing, RediSearch support, SSL/TLS encryption, and production-ready features.

## Installation

```bash
pip install hivemind-redis-database
```

## Compatibility

- Python 3.10 or newer
- Redis for single-instance mode
- Redis Cluster for cluster mode
- Redis Stack or Redis with the RediSearch module loaded for advanced search

## Configuration

Add to your `server.json` configuration file:
HiveMind passes these values to the plugin when it loads the database backend.

### Single Redis Instance

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
      "max_connections": 10
    }
  }
}
```

### Redis Cluster

```json
{
  "database": {
    "module": "hivemind-redis-db-plugin",
    "hivemind-redis-db-plugin": {
      "name": "clients",
      "subfolder": "hivemind-core",
      "cluster_nodes": [
        {"host": "redis-node1", "port": 6379},
        {"host": "redis-node2", "port": 6380},
        {"host": "redis-node3", "port": 6381}
      ],
      "cluster_hash_tag": "clients",
      "password": "your_password",
      "max_connections": 20
    }
  }
}
```

### SSL/TLS Configuration

```json
{
  "database": {
    "module": "hivemind-redis-db-plugin",
    "hivemind-redis-db-plugin": {
      "name": "clients",
      "subfolder": "hivemind-core",
      "host": "redis.example.com",
      "port": 6380,
      "ssl": true,
      "ssl_certfile": "/path/to/client.crt",
      "ssl_keyfile": "/path/to/client.key",
      "ssl_ca_certs": "/path/to/ca.crt",
      "ssl_cert_reqs": "required",
      "ssl_check_hostname": true
    }
  }
}
```

### Configuration Parameters

- `name`: Database name from the HiveMind-core database contract (default: "clients")
- `subfolder`: HiveMind-core config namespace value, accepted for compatibility (default: "hivemind-core")
- `host`: Redis server hostname (default: "127.0.0.1")
- `port`: Redis server port (default: 6379)
- `db`: Redis database number (default: 0)
- `password`: Redis password for authentication
- `username`: Redis username for ACL authentication (default: "default")
- `cluster_nodes`: List of cluster nodes (for Redis Cluster mode)
- `cluster_hash_tag`: Optional fixed Redis Cluster hash tag for single-slot atomic writes in cluster mode
- `max_connections`: Maximum connection pool size (default: 5)
- `index_prefix`: Prefix for index keys (default: "client")
- `ssl` or `use_ssl`: Enable SSL/TLS connection (default: false)
- `ssl_certfile`: Path to SSL certificate file
- `ssl_keyfile`: Path to SSL private key file
- `ssl_ca_certs`: Path to CA certificates file
- `ssl_cert_reqs`: SSL certificate requirements ("required", "optional", "none") (default: "required")
- `ssl_check_hostname`: Verify SSL hostname (default: true)

Revoked clients are excluded from iteration and search results so HiveMind-core admin commands only operate on active clients.

## Usage

```python
from hivemind_redis_database import RedisDB

# Manual configuration for a local Redis instance
db = RedisDB(host="127.0.0.1", port=6379)

# HiveMind loads the same settings from server.json
# and passes them to this plugin automatically.

# Cluster via explicit startup nodes
cluster_db = RedisDB(
    cluster_nodes=[
        {"host": "redis-node1", "port": 6379},
        {"host": "redis-node2", "port": 6379},
    ],
    cluster_hash_tag="clients",
    password="your_password",
)

# Manual configuration with SSL
db = RedisDB(
    host="redis.example.com",
    port=6380,
    ssl=True,
    ssl_certfile="/path/to/client.crt",
    ssl_keyfile="/path/to/client.key",
    ssl_ca_certs="/path/to/ca.crt"
)
```

## Features

- ✅ **Auto-Detection**: Redis vs Redis Cluster
- ✅ **RediSearch Support**: Advanced search with fallback
- ✅ **Connection Pooling**: Efficient resource management
- ✅ **Health Checks**: Automatic connection validation
- ✅ **SSL/TLS Support**: Secure connections with certificate validation
- ✅ **Multi-Hub Support**: Configurable key prefixes for multiple instances
- ✅ **Production Ready**: Comprehensive error handling and logging

## Cluster Note

Redis Cluster support has two modes:

- default legacy mode: compatible with existing deployments, but multi-key writes are best-effort across slots
- `cluster_hash_tag` mode: keeps all keys for that namespace in one slot and enables transactional cluster pipelines

Use `cluster_hash_tag` for new cluster deployments. The recommended migration
path for existing cluster deployments is
documented in [docs/cluster_consistency.md](docs/cluster_consistency.md).

For an existing Redis Cluster deployment, do not just flip `cluster_hash_tag`
in place. Migrate the namespace first:

1. Stop HiveMind writers for a short maintenance window.
2. Back up the current Redis namespace.
3. Run a dry run to confirm the source and target namespaces.
4. Run the migration command to copy records into the tagged namespace.
5. Update `server.json` to set `cluster_hash_tag`.
6. Restart HiveMind and smoke-test add/get/update/revoke/search behavior.
7. Keep the legacy namespace for rollback until the new deployment is stable.

Dry run:

```bash
hivemind-redis-migrate-cluster \
  --config ~/.config/hivemind-core/server.json \
  --target-cluster-hash-tag clients \
  --dry-run
```

Migration command:

```bash
hivemind-redis-migrate-cluster \
  --config ~/.config/hivemind-core/server.json \
  --target-cluster-hash-tag clients \
  --clear-target
```
