# HiveMind Redis Database

Redis database plugin for HiveMind with consistent indexing, RediSearch support, SSL/TLS encryption, and production-ready features.

## Installation

```bash
pip install hivemind-redis-database
```

## Configuration

Add to your `server.json` configuration file:

### Single Redis Instance

```json
{
  "database": {
    "module": "hivemind-redis-db-plugin",
    "hivemind-redis-db-plugin": {
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
      "cluster_nodes": [
        {"host": "redis-node1", "port": 6379},
        {"host": "redis-node2", "port": 6380},
        {"host": "redis-node3", "port": 6381}
      ],
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

- `host`: Redis server hostname (default: "127.0.0.1")
- `port`: Redis server port (default: 6379)
- `db`: Redis database number (default: 0)
- `password`: Redis password for authentication
- `username`: Redis username for ACL authentication (default: "default")
- `cluster_nodes`: List of cluster nodes (for Redis Cluster mode)
- `max_connections`: Maximum connection pool size (default: 5)
- `index_prefix`: Prefix for index keys (default: "client")
- `ssl`: Enable SSL/TLS connection (default: false)
- `ssl_certfile`: Path to SSL certificate file
- `ssl_keyfile`: Path to SSL private key file
- `ssl_ca_certs`: Path to CA certificates file
- `ssl_cert_reqs`: SSL certificate requirements ("required", "optional", "none") (default: "required")
- `ssl_check_hostname`: Verify SSL hostname (default: true)

## Usage

```python
from hivemind_redis_database import RedisDB

# Using configuration from server.json
db = RedisDB()

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
