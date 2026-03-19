from dataclasses import dataclass
from typing import List, Optional, Iterable, Union
import json
import time

import redis
from redis.cluster import ClusterNode
from ovos_utils.log import LOG

from hivemind_plugin_manager.database import Client, AbstractRemoteDB, cast2client


@dataclass
class RedisDB(AbstractRemoteDB):
    """
    Redis database implementation for HiveMind with advanced features.

    This class provides a high-performance Redis-based database solution for HiveMind,
    supporting both single Redis instances and Redis Cluster configurations. It includes
    automatic RediSearch integration, connection pooling, and comprehensive error handling.

    Features:
        - Automatic Redis vs Redis Cluster detection and configuration
        - RediSearch integration with fallback to basic indexing
        - Connection pooling with health checks and retry logic
        - Sequential client ID generation
        - Multi-hub support via configurable key prefixes
        - Production-ready error handling and logging

    Attributes:
        host (str): Redis server hostname (default: "127.0.0.1")
        port (int): Redis server port (default: 6379)
        name (str): Database name identifier (default: "clients")
        password (Optional[str]): Redis authentication password
        username (Optional[str]): Redis authentication username (default: "default")
        db (Optional[int]): Redis database number for single instance
        cluster_nodes (Optional[List[dict]]): Redis Cluster node configuration
        index_prefix (str): Key prefix for all database operations (default: "client")
        max_connections (int): Maximum connection pool size (default: 5)
        retry_attempts (int): Number of retry attempts (default: 3)
        retry_delay (float): Delay between retry attempts in seconds (default: 0.1)
        use_ssl (bool): Enable SSL/TLS connection (default: False)
        ssl_certfile (Optional[str]): Path to SSL certificate file
        ssl_keyfile (Optional[str]): Path to SSL private key file
        ssl_ca_certs (Optional[str]): Path to CA certificates file
        ssl_cert_reqs (str): SSL certificate requirements ("required", "optional", "none") (default: "required")
        ssl_check_hostname (bool): Verify SSL hostname (default: True)
    """
    host: str = "127.0.0.1"
    port: int = 6379
    name: str = "clients"
    password: Optional[str] = None
    username: Optional[str] = "default"
    db: Optional[int] = 0
    cluster_nodes: Optional[List[dict]] = None
    index_prefix: str = "client"
    max_connections: int = 5
    retry_attempts: int = 3
    retry_delay: float = 0.1
    use_ssl: bool = False
    ssl: Optional[bool] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None
    ssl_ca_certs: Optional[str] = None
    ssl_cert_reqs: str = "required"
    ssl_check_hostname: bool = True


    def __post_init__(self):
        """
        Initialize the RedisDB connection with automatic cluster/single instance detection.
        """
        self._normalize_parameters()
        self._validate_parameters()
        LOG.info("Redis database initialized with hiredis for optimal performance")

        self.is_cluster = self._detect_cluster()
        if self.is_cluster:
            LOG.info("Redis Cluster detected, using cluster client")
            self.redis = self._create_cluster_connection()
        else:
            LOG.info("Single Redis instance detected, using standard client")
            self.redis = self._create_single_connection()

        counter_key = f"{self.index_prefix}:count"
        if self.redis.setnx(counter_key, 0):
            LOG.debug(f"Initialized client counter to 0 for {self.index_prefix}")

        self.redisearch_available = self._check_redisearch_availability()
        if self.redisearch_available:
            self._setup_redisearch_index()
            LOG.info("RediSearch module detected, enabling advanced search features")
        else:
            LOG.info("RediSearch module not available, using basic indexing")

        if not self.health_check():
            LOG.error("Redis connection health check failed during initialization")
            raise redis.ConnectionError("Failed to establish healthy Redis connection")

    def _normalize_parameters(self):
        """Normalize optional parameters and backward-compatible aliases."""
        if self.host is None:
            self.host = "127.0.0.1"
        if self.port is None:
            self.port = 6379
        if self.db is None:
            self.db = 0
        if self.username is None:
            self.username = "default"
        if self.name is None:
            self.name = "clients"
        if self.index_prefix is None:
            self.index_prefix = "client"
        if self.ssl is not None:
            self.use_ssl = bool(self.ssl)

    def _validate_parameters(self):
        """
        Validate input parameters to ensure they are within acceptable ranges.
        """
        if not isinstance(self.port, int) or not (1 <= self.port <= 65535):
            raise ValueError(f"Port must be an integer between 1 and 65535, got {self.port}")

        if not isinstance(self.db, int) or self.db < 0:
            raise ValueError(f"Database ID must be a non-negative integer, got {self.db}")

        if not isinstance(self.max_connections, int) or self.max_connections < 1:
            raise ValueError(f"max_connections must be a positive integer, got {self.max_connections}")

        if not isinstance(self.retry_attempts, int) or self.retry_attempts < 0:
            raise ValueError(f"retry_attempts must be a non-negative integer, got {self.retry_attempts}")

        if not isinstance(self.retry_delay, (int, float)) or self.retry_delay < 0:
            raise ValueError(f"retry_delay must be a non-negative number, got {self.retry_delay}")

        if self.username and not isinstance(self.username, str):
            raise ValueError(f"Username must be a string, got {type(self.username)}")

        if self.password and not isinstance(self.password, str):
            raise ValueError(f"Password must be a string, got {type(self.password)}")

        if self.ssl_cert_reqs not in ["required", "optional", "none"]:
            raise ValueError(f"ssl_cert_reqs must be 'required', 'optional', or 'none', got {self.ssl_cert_reqs}")

    def _get_ssl_kwargs(self) -> dict:
        """
        Build SSL kwargs compatible with redis-py clients.

        Returns:
            Keyword arguments for SSL-enabled Redis connections.
        """
        if not self.use_ssl:
            return {}

        check_hostname = self.ssl_check_hostname
        if self.ssl_cert_reqs == "none":
            check_hostname = False

        ssl_kwargs = {
            "ssl": True,
            "ssl_cert_reqs": self.ssl_cert_reqs,
            "ssl_check_hostname": check_hostname,
        }
        if self.ssl_certfile:
            ssl_kwargs["ssl_certfile"] = self.ssl_certfile
        if self.ssl_keyfile:
            ssl_kwargs["ssl_keyfile"] = self.ssl_keyfile
        if self.ssl_ca_certs:
            ssl_kwargs["ssl_ca_certs"] = self.ssl_ca_certs
        return ssl_kwargs

    def _get_startup_nodes(self) -> List[ClusterNode]:
        """Normalize startup nodes into redis-py ClusterNode objects."""
        raw_nodes = self.cluster_nodes or [{"host": self.host, "port": self.port}]
        startup_nodes = []
        for node in raw_nodes:
            if isinstance(node, ClusterNode):
                startup_nodes.append(node)
            elif isinstance(node, dict):
                startup_nodes.append(ClusterNode(node["host"], int(node["port"])))
            else:
                raise ValueError(f"Unsupported cluster node type: {type(node)}")
        return startup_nodes

    def _detect_cluster(self) -> bool:
        """
        Detect if Redis is running in cluster mode.

        Returns:
            True if cluster mode is detected, False otherwise.
        """
        if self.cluster_nodes:
            return True

        try:
            connection_kwargs = {
                "host": self.host,
                "port": self.port,
                "password": self.password,
                "username": self.username,
                "db": 0,
                "socket_connect_timeout": 2,
                "socket_timeout": 2,
            }
            connection_kwargs.update(self._get_ssl_kwargs())

            test_client = redis.StrictRedis(**connection_kwargs)
            test_client.ping()

            info = test_client.info('cluster')
            is_cluster_enabled = info.get('cluster_enabled', 0) == 1

            test_client.close()
            return is_cluster_enabled

        except Exception as e:
            LOG.debug(f"Cluster detection failed: {e}")
            return False

    def _create_single_connection(self) -> redis.StrictRedis:
        """
        Create connection to single Redis instance.

        Returns:
            Redis client instance
        """
        connection_kwargs = {
            'host': self.host,
            'port': self.port,
            'db': self.db,
            'password': self.password if self.password else None,
            'username': self.username,
            'decode_responses': True,
            'socket_connect_timeout': 5,
            'socket_timeout': 5,
            'health_check_interval': 30,
            'max_connections': self.max_connections,
        }
        connection_kwargs.update(self._get_ssl_kwargs())

        client = redis.StrictRedis(
            **connection_kwargs,
            retry=redis.retry.Retry(
                redis.backoff.ExponentialBackoff(base=self.retry_delay),
                retries=self.retry_attempts
            ),
            retry_on_error=[redis.ConnectionError, redis.TimeoutError],
        )
        self.redis_pool = client.connection_pool
        return client

    def _create_cluster_connection(self) -> redis.RedisCluster:
        """
        Create connection to Redis Cluster.

        Returns:
            RedisCluster client instance
        """
        startup_nodes = self._get_startup_nodes()
        connection_kwargs = {
            "startup_nodes": startup_nodes,
            "password": self.password,
            "username": self.username,
            "decode_responses": True,
            "socket_connect_timeout": 5,
            "socket_timeout": 5,
            "max_connections": self.max_connections,
            "skip_full_coverage_check": True,
        }
        connection_kwargs.update(self._get_ssl_kwargs())

        try:
            return redis.RedisCluster(**connection_kwargs)
        except Exception as e:
            LOG.warning(f"Failed to create cluster connection: {e}")
            raise

    def _check_redisearch_availability(self) -> bool:
        """
        Check if RediSearch module is available on the Redis server.

        Returns:
            True if RediSearch is available, False otherwise.
        """
        try:
            modules = self.redis.execute_command("MODULE", "LIST")
            for module in modules:
                name = None
                if isinstance(module, dict):
                    name = module.get("name")
                elif isinstance(module, (list, tuple)):
                    for i in range(0, len(module) - 1, 2):
                        if str(module[i]).lower() == "name":
                            name = module[i + 1]
                            break
                if str(name).lower() == "search":
                    return True
        except Exception as e:
            LOG.debug(f"RediSearch module check failed: {e}")

        try:
            self.redis.execute_command("FT._LIST")
            return True
        except Exception:
            return False

    def _setup_redisearch_index(self):
        """
        Set up RediSearch index for clients if available.
        """
        try:
            index_name = f"{self.index_prefix}_search_index"
            self.redis.execute_command(
                "FT.CREATE", index_name,
                "ON", "HASH",
                "PREFIX", "1", f"{self.index_prefix}:idx:",
                "SCHEMA",
                "name", "TEXT",
                "api_key", "TEXT"
            )
            LOG.debug(f"Created RediSearch index '{index_name}'")
        except redis.ResponseError as e:
            if "Index already exists" in str(e):
                LOG.debug(f"RediSearch index '{index_name}' already exists")
            else:
                LOG.warning(f"Failed to create RediSearch index: {e}")
        except Exception as e:
            LOG.warning(f"Error setting up RediSearch index: {e}")

    def _get_next_client_id(self) -> int:
        """
        Generate the next sequential client ID using Redis INCR for atomicity.

        Returns:
            The next available client ID
        """
        return int(self.redis.incr(f"{self.index_prefix}:id_seq"))

    def add_item(self, client: Client) -> bool:
        """
        Add a new client to the Redis database or update existing client.

        Args:
            client: The client object to add or update.

        Returns:
            True if the client was added/updated successfully, False otherwise.
        """
        create_marker = "__hivemind_creating__"
        claimed_key = None
        try:
            raw_client_id = getattr(client, "client_id", None)
            client_id_was_provided = isinstance(raw_client_id, int)

            # Ensure client has a valid ID
            if not hasattr(client, 'client_id') or client.client_id is None:
                client.client_id = self._get_next_client_id()
            elif isinstance(client.client_id, str):
                try:
                    client.client_id = int(client.client_id)
                    client_id_was_provided = True
                except ValueError:
                    client.client_id = self._get_next_client_id()
            elif not isinstance(client.client_id, int):
                client.client_id = self._get_next_client_id()

            # Ensure client attributes are initialized
            self._ensure_client_attributes(client)
            retry_delay = getattr(self, "retry_delay", 0.1)

            while True:
                item_key = f"{self.index_prefix}:client:{client.client_id}"
                if self.redis.setnx(item_key, create_marker):
                    claimed_key = item_key
                    break

                existing_data = self.redis.get(item_key)
                if existing_data == create_marker:
                    time.sleep(retry_delay)
                    continue

                if client_id_was_provided:
                    LOG.debug(f"Client '{client.client_id}' already exists, updating instead of creating new")
                    return self.update_client(client)

                client.client_id = self._get_next_client_id()

            serialized_client = client.serialize()

            # Use pipeline for atomic operations
            p = self.redis.pipeline()
            p.set(item_key, serialized_client)
            p.sadd(f"{self.index_prefix}:name:{client.name}", str(client.client_id))
            p.sadd(f"{self.index_prefix}:api_key:{client.api_key}", str(client.client_id))
            # Feed RediSearch doc
            p.hset(f"{self.index_prefix}:idx:{client.client_id}", mapping={
                "name": client.name,
                "api_key": client.api_key,
            })
            p.incr(f"{self.index_prefix}:count")
            p.execute()
            
            LOG.debug(f"Successfully added client '{client.client_id}'")
            return True
        except Exception as e:
            if claimed_key and self.redis.get(claimed_key) == create_marker:
                self.redis.delete(claimed_key)
            LOG.error(f"Failed to add client: {e}")
            return False

    def delete_item(self, client: Client) -> bool:
        """
        Revoke a client's credentials by updating their record.
        This is the method called by hivemind-core delete-client command.

        Args:
            client: The client object to revoke.

        Returns:
            True if the revocation was successful, False otherwise.
        """
        return self.remove_client(str(client.client_id))

    def remove_client(self, client_id: str) -> bool:
        """
        Revoke a client's credentials by updating their record.

        Args:
            client_id: The ID of the client to revoke.

        Returns:
            True if the revocation was successful, False otherwise.
        """
        try:
            item_key = f"{self.index_prefix}:client:{client_id}"
            client_data = self.redis.get(item_key)
            if not client_data:
                LOG.warning(f"Client '{client_id}' not found for revocation")
                return False

            # Parse the serialized data (assuming JSON)
            data = json.loads(client_data)
            old_name = data.get('name', '')
            old_api_key = data.get('api_key', '')

            # Revoke credentials
            data['name'] = ""
            data['api_key'] = "revoked"
            data['password'] = None
            data['crypto_key'] = None

            p = self.redis.pipeline()
            p.set(item_key, json.dumps(data))

            # Update indices
            p.srem(f"{self.index_prefix}:name:{old_name}", client_id)
            p.srem(f"{self.index_prefix}:api_key:{old_api_key}", client_id)
            p.sadd(f"{self.index_prefix}:api_key:revoked", client_id)
            # Update RediSearch doc
            p.hset(f"{self.index_prefix}:idx:{client_id}", mapping={
                "name": "",
                "api_key": "revoked",
            })

            p.execute()

            LOG.info(f"Successfully revoked client '{client_id}'")
            return True
        except json.JSONDecodeError as e:
            LOG.error(f"Failed to parse client data for '{client_id}': {e}")
            return False
        except Exception as e:
            LOG.error(f"Unexpected error while revoking client '{client_id}': {e}")
            return False

    def _ensure_client_attributes(self, client: Client):
        """
        Ensure client has required attributes initialized.

        Args:
            client: The client object to initialize attributes for
        """
        for attr in ['message_blacklist', 'intent_blacklist', 'skill_blacklist']:
            if not hasattr(client, attr) or getattr(client, attr) is None:
                setattr(client, attr, [])

    def update_client(self, client: Client) -> bool:
        """
        Update an existing client's information in Redis.

        Args:
            client: The client object with updated information.

        Returns:
            True if the update was successful, False otherwise.
        """
        try:
            item_key = f"{self.index_prefix}:client:{client.client_id}"
            old_client_data = self.redis.get(item_key)
            if not old_client_data:
                LOG.warning(f"Client '{client.client_id}' not found for update")
                return False

            old_client = cast2client(old_client_data)
            self._ensure_client_attributes(client)

            p = self.redis.pipeline()
            p.set(item_key, client.serialize())
            
            # Update indices only if values changed
            if old_client.name != client.name:
                p.srem(f"{self.index_prefix}:name:{old_client.name}", str(client.client_id))
                p.sadd(f"{self.index_prefix}:name:{client.name}", str(client.client_id))
            if old_client.api_key != client.api_key:
                p.srem(f"{self.index_prefix}:api_key:{old_client.api_key}", str(client.client_id))
                p.sadd(f"{self.index_prefix}:api_key:{client.api_key}", str(client.client_id))
            # Update RediSearch doc
            p.hset(f"{self.index_prefix}:idx:{client.client_id}", mapping={
                "name": client.name,
                "api_key": client.api_key,
            })
            
            p.execute()
            LOG.debug(f"Successfully updated client '{client.client_id}'")
            return True
        except Exception as e:
            LOG.error(f"Failed to update client '{client.client_id}': {e}")
            return False

    def _search_with_redisearch(self, key: str, val: str) -> List[Client]:
        """
        Search using RediSearch if available.

        Args:
            key: The field to search by
            val: The value to search for

        Returns:
            List of matching clients
        """
        try:
            index_name = f"{self.index_prefix}_search_index"
            def _esc(v: str) -> str:
                return v.replace('\\', '\\\\').replace('"', '\\"').replace(':', '\\:')
            query = f'@{key}:"{_esc(str(val))}"'
            results = self.redis.execute_command("FT.SEARCH", index_name, query)
            res = []
            if results and len(results) > 1:
                for i in range(1, len(results), 2):
                    doc_key = results[i]
                    if doc_key.startswith(f"{self.index_prefix}:idx:"):
                        client_id = doc_key.split(":")[-1]
                        client_key = f"{self.index_prefix}:client:{client_id}"
                        client_data = self.redis.get(client_key)
                        if client_data:
                            try:
                                client = cast2client(client_data)
                                if client.api_key != "revoked":
                                    res.append(client)
                            except Exception as e:
                                LOG.warning(f"Failed to deserialize client data: {e}")
            return res
        except Exception:
            return []

    def _search_with_index(self, key: str, val: str) -> List[Client]:
        """
        Search using Redis sets for indexed fields.

        Args:
            key: The field to search by
            val: The value to search for

        Returns:
            List of matching clients
        """
        LOG.debug(f"Searching for clients by indexed field '{key}' with value '{val}'")
        client_ids = self.redis.smembers(f"{self.index_prefix}:{key}:{val}")
        res = []
        for cid in client_ids:
            try:
                client_data = self.redis.get(f"{self.index_prefix}:client:{cid}")
                if client_data:
                    client = cast2client(client_data)
                    if client.api_key != "revoked":
                        res.append(client)
            except Exception as e:
                LOG.warning(f"Failed to deserialize client data: {e}")
        LOG.debug(f"Found {len(res)} clients matching '{key}={val}'")
        return res

    def _search_brute_force(self, key: str, val) -> List[Client]:
        """
        Fallback search by scanning all clients.

        Args:
            key: The field to search by
            val: The value to search for

        Returns:
            List of matching clients
        """
        res = []
        for client_id in self.redis.scan_iter(f"{self.index_prefix}:client:*", count=100):
            try:
                client_data = self.redis.get(client_id)
                if client_data:
                    client = cast2client(client_data)
                    if hasattr(client, key) and getattr(client, key) == val and client.api_key != "revoked":
                        res.append(client)
            except Exception as e:
                LOG.warning(f"Failed to deserialize client data: {e}")
                continue
        return res

    def search_by_value(self, key: str, val) -> List[Client]:
        """
        Search for clients by a specific key-value pair in Redis.

        Args:
            key: The key to search by.
            val: The value to search for.

        Returns:
            A list of clients that match the search criteria.
        """
        if self.redisearch_available and key in ['name', 'api_key']:
            redisearch_results = self._search_with_redisearch(key, val)
            if redisearch_results:
                return redisearch_results

        if key in ['name', 'api_key']:
            return self._search_with_index(key, val)

        return self._search_brute_force(key, val)

    def __len__(self) -> int:
        """
        Get the number of items in the Redis database.

        Returns:
            The number of clients in the database.
        """
        counter_key = f"{self.index_prefix}:count"
        try:
            count = self.redis.get(counter_key)
            if count is not None:
                return int(count)
            self.redis.setnx(counter_key, 0)
            return 0
        except Exception as e:
            LOG.warning(f"Failed to get client count from counter: {e}")
            return 0

    def __iter__(self) -> Iterable['Client']:
        """
        Iterate over all clients in Redis.

        Returns:
            An iterator over the clients in the database.
        """
        for client_id in self.redis.scan_iter(f"{self.index_prefix}:client:*", count=100):
            try:
                client_data = self.redis.get(client_id)
                if not client_data or client_data == "__hivemind_creating__":
                    continue

                client = cast2client(client_data)
                if client.api_key == "revoked":
                    continue

                for attr in ['message_blacklist', 'intent_blacklist', 'skill_blacklist']:
                    if not hasattr(client, attr) or getattr(client, attr) is None:
                        setattr(client, attr, [])
                yield client
            except Exception as e:
                LOG.error(f"Failed to get client '{client_id}' : {e}")

    def __del__(self):
        """
        Cleanup resources when the object is destroyed.
        """
        try:
            if hasattr(self, 'redis_pool') and self.redis_pool:
                self.redis_pool.disconnect()
        except Exception as e:
            LOG.error(f"Error during Redis pool cleanup: {e}")

    def health_check(self) -> bool:
        """
        Perform a health check on the Redis connection.

        Returns:
            True if Redis is healthy, False otherwise
        """
        try:
            self.redis.ping()
            return True
        except Exception as e:
            LOG.error(f"Redis health check failed: {e}")
            return False
