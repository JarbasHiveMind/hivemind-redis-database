# HiveMind Redis Database

Redis database plugin for HiveMind with RediSearch support. Automatically detects and supports both single Redis instances and Redis Clusters.

## Installation

```bash
pip install hivemind-redis-database
```

## Configuration

### Single Redis Instance

Add to your `server.json`:

```json
{
  "database": {
    "module": "hivemind-redis-db-plugin",
    "hivemind-redis-db-plugin": {
      "host": "127.0.0.1",
      "port": 6379,
      "db": 0
    }
  }
}
```

### Redis Cluster

For Redis Cluster, specify cluster nodes:

```json
{
  "database": {
    "module": "hivemind-redis-db-plugin",
    "hivemind-redis-db-plugin": {
      "cluster_nodes": [
        {"host": "redis-node1", "port": 6379},
        {"host": "redis-node2", "port": 6380},
        {"host": "redis-node3", "port": 6381}
      ]
    }
  }
}
```

## Usage

### Automatic Detection

The plugin automatically detects Redis type:

```python
from hivemind_redis_database import RedisDB

# Single instance
db = RedisDB(host="127.0.0.1", port=6379, db=0)

# Cluster (auto-detected)
db = RedisDB(cluster_nodes=[
    {"host": "node1", "port": 6379},
    {"host": "node2", "port": 6380}
])
```

### Manual Configuration

```python
# Force cluster mode
db = RedisDB(
    cluster_nodes=[{"host": "cluster-node", "port": 6379}]
)
```
