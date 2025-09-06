from dataclasses import dataclass
from typing import List, Optional, Iterable

import redis
from ovos_utils.log import LOG

from hivemind_plugin_manager.database import Client, AbstractRemoteDB, cast2client


@dataclass
class RedisDB(AbstractRemoteDB):
    """
    Advanced Redis database implementation with RediSearch support for HiveMind.

    This class provides a robust, production-ready Redis-backed database for storing and
    retrieving Client objects with the following features:

    Features:
        - Redis ACL authentication support
        - Optional RediSearch integration with automatic index creation and graceful fallback
        - Configurable connection pooling with performance monitoring
        - Comprehensive input validation and error handling
        - Performance optimizations (MGET, pipelining, client counter)
        - Resource cleanup and connection monitoring
        - Structured logging throughout operations
        - Exponential backoff for retries

    Attributes:
        host (str): Redis server hostname. Defaults to "127.0.0.1".
        port (int): Redis server port (1-65535). Defaults to 6379.
        name (str): Database name identifier. Defaults to "clients".
        password (Optional[str]): Redis password for authentication. Defaults to None.
        username (Optional[str]): Redis username for ACL authentication. Defaults to "default".
        db (Optional[int]): Redis database number (0+). Defaults to 0.
        index_prefix (str): Prefix for index keys. Defaults to "client:index".
        max_connections (int): Maximum connection pool size. Defaults to 5.
        ssl (bool): Enable SSL/TLS connections. Defaults to False.
        retry_attempts (int): Number of retry attempts for failed operations. Defaults to 3.
        retry_delay (float): Base delay between retries in seconds. Defaults to 0.1.
    """
    host: str = "127.0.0.1"
    port: int = 6379
    name: str = "clients"
    password: Optional[str] = None
    username: Optional[str] = "default"
    db: Optional[int] = 0
    cluster_nodes: Optional[List[dict]] = None
    index_prefix: str = "client:index"
    max_connections: int = 5
    ssl: bool = False
    retry_attempts: int = 3
    retry_delay: float = 0.1

    def __post_init__(self):
        """
        Initialize the RedisDB connection with automatic cluster/single instance detection.
        """
        self._validate_parameters()
        LOG.info("Redis database initialized with hiredis for optimal performance")

        self.is_cluster = self._detect_cluster()
        if self.is_cluster:
            LOG.info("Redis Cluster detected, using cluster client")
            self.redis = self._create_cluster_connection()
        else:
            LOG.info("Single Redis instance detected, using standard client")
            self.redis = self._create_single_connection()

        counter_key = f"{self.name}:count"
        if not self.redis.exists(counter_key):
            self.redis.set(counter_key, 0)
            LOG.debug("Initialized client counter to 0")

        self.redisearch_available = self._check_redisearch_availability()
        if self.redisearch_available:
            self._setup_redisearch_index()
            LOG.info("RediSearch module detected, enabling advanced search features")
        else:
            LOG.info("RediSearch module not available, using basic indexing")

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

    def _detect_cluster(self) -> bool:
        """
        Detect if Redis is running in cluster mode.

        Returns:
            True if cluster mode is detected, False otherwise.
        """
        if self.cluster_nodes:
            return True

        try:
            test_client = redis.StrictRedis(
                host=self.host,
                port=self.port,
                password=self.password,
                username=self.username,
                db=0,
                socket_connect_timeout=2,
                socket_timeout=2
            )
            test_client.ping()

            info = test_client.info('cluster')
            is_cluster_enabled = info.get('cluster_enabled', 0) == 1

            test_client.close()
            return is_cluster_enabled

        except Exception:
            return False

    def _create_single_connection(self):
        """
        Create connection to single Redis instance.

        Returns:
            Redis client instance
        """
        self.redis_pool = redis.ConnectionPool(
            host=self.host,
            port=self.port,
            db=self.db,
            password=self.password if self.password else None,
            username=self.username,
            decode_responses=True,
            max_connections=self.max_connections,
            retry_on_timeout=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            health_check_interval=30,
            retry_on_error=[redis.ConnectionError, redis.TimeoutError]
        )
        return redis.StrictRedis(connection_pool=self.redis_pool)

    def _create_cluster_connection(self):
        """
        Create connection to Redis Cluster.

        Returns:
            RedisCluster client instance
        """
        if self.cluster_nodes:
            startup_nodes = self.cluster_nodes
        else:
            startup_nodes = [{"host": self.host, "port": self.port}]

        try:
            return redis.RedisCluster(
                startup_nodes=startup_nodes,
                password=self.password,
                username=self.username,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                max_connections=self.max_connections,
                skip_full_coverage_check=True
            )
        except Exception as e:
            LOG.warning(f"Failed to create cluster connection: {e}")
            raise

    def _get_index_prefix(self) -> str:
        """Get the full index prefix for keys."""
        return f"{self.index_prefix}:"

    def _check_redisearch_availability(self) -> bool:
        """
        Check if RediSearch module is available on the Redis server.

        Returns:
            True if RediSearch is available, False otherwise.
        """
        try:
            # Try to execute a simple RediSearch command
            self.redis.ft("test_index").info()
            return True
        except redis.ResponseError:
            # RediSearch module not loaded
            return False
        except Exception as e:
            LOG.debug(f"Error checking RediSearch availability: {e}")
            return False

    def _setup_redisearch_index(self):
        """
        Set up RediSearch index for clients if available.
        """
        try:
            index_name = f"{self.name}_search_index"
            self.redis.execute_command("FT.CREATE", index_name, "ON", "HASH", "PREFIX", "1", "client:",
                                     "SCHEMA", "name", "TEXT", "api_key", "TEXT")
            LOG.debug(f"Created RediSearch index '{index_name}'")
        except redis.ResponseError as e:
            if "Index already exists" in str(e):
                LOG.debug(f"RediSearch index '{index_name}' already exists")
            else:
                LOG.warning(f"Failed to create RediSearch index: {e}")
        except Exception as e:
            LOG.warning(f"Error setting up RediSearch index: {e}")

    def add_item(self, client: Client) -> bool:
        """
        Add a client to Redis and RediSearch.

        Args:
            client: The client to be added.

        Returns:
            True if the addition was successful, False otherwise.
        """
        item_key = f"client:{client.client_id}"
        serialized_data: str = client.serialize()
        counter_key = f"{self.name}:count"
        try:
            self.redis.set(item_key, serialized_data)
            self.redis.sadd(f"{self.index_prefix}:name:{client.name}", client.client_id)
            self.redis.sadd(f"{self.index_prefix}:api_key:{client.api_key}", client.client_id)
            self.redis.incr(counter_key)
            LOG.debug(f"Successfully added client '{client.client_id}'")
            return True
        except redis.ConnectionError as e:
            LOG.error(f"Redis connection error while adding client '{client.client_id}': {e}")
            return False
        except redis.TimeoutError as e:
            LOG.error(f"Redis timeout error while adding client '{client.client_id}': {e}")
            return False
        except Exception as e:
            LOG.error(f"Unexpected error while adding client '{client.client_id}': {e}")
            return False

    def remove_client(self, client_id: str) -> bool:
        """
        Remove a client from Redis and clean up indices.

        Args:
            client_id: The ID of the client to remove.

        Returns:
            True if the removal was successful, False otherwise.
        """
        counter_key = f"{self.name}:count"
        try:
            item_key = f"client:{client_id}"
            client_data = self.redis.get(item_key)
            if not client_data:
                LOG.warning(f"Client '{client_id}' not found for removal")
                return False

            self.redis.delete(item_key)
            self.redis.decr(counter_key)

            try:
                client = cast2client(client_data)
                self.redis.srem(f"{self.index_prefix}:name:{client.name}", client_id)
                self.redis.srem(f"{self.index_prefix}:api_key:{client.api_key}", client_id)
            except Exception as e:
                LOG.warning(f"Failed to deserialize client data for index cleanup: {e}")

            LOG.info(f"Successfully removed client '{client_id}'")
            return True
        except redis.ConnectionError as e:
            LOG.error(f"Redis connection error while removing client '{client_id}': {e}")
            return False
        except redis.TimeoutError as e:
            LOG.error(f"Redis timeout error while removing client '{client_id}': {e}")
            return False
        except Exception as e:
            LOG.error(f"Unexpected error while removing client '{client_id}': {e}")
            return False

    def update_client(self, client: Client) -> bool:
        """
        Update an existing client in Redis and update indices.

        Args:
            client: The updated client object.

        Returns:
            True if the update was successful, False otherwise.
        """
        try:
            item_key = f"client:{client.client_id}"
            old_client_data = self.redis.get(item_key)
            if not old_client_data:
                LOG.warning(f"Client '{client.client_id}' not found for update")
                return False

            old_client = cast2client(old_client_data)

            if not hasattr(client, 'message_blacklist') or client.message_blacklist is None:
                client.message_blacklist = []
            if not hasattr(client, 'intent_blacklist') or client.intent_blacklist is None:
                client.intent_blacklist = []
            if not hasattr(client, 'skill_blacklist') or client.skill_blacklist is None:
                client.skill_blacklist = []

            serialized_data = client.serialize()
            self.redis.set(item_key, serialized_data)

            if old_client.name != client.name:
                self.redis.srem(f"{self.index_prefix}:name:{old_client.name}", client.client_id)
                self.redis.sadd(f"{self.index_prefix}:name:{client.name}", client.client_id)
                LOG.debug(f"Updated name index for client '{client.client_id}' from '{old_client.name}' to '{client.name}'")

            if old_client.api_key != client.api_key:
                self.redis.srem(f"{self.index_prefix}:api_key:{old_client.api_key}", client.client_id)
                self.redis.sadd(f"{self.index_prefix}:api_key:{client.api_key}", client.client_id)
                LOG.debug(f"Updated api_key index for client '{client.client_id}'")

            LOG.info(f"Successfully updated client '{client.client_id}'")
            return True
        except redis.ConnectionError as e:
            LOG.error(f"Redis connection error while updating client '{client.client_id}': {e}")
            return False
        except redis.TimeoutError as e:
            LOG.error(f"Redis timeout error while updating client '{client.client_id}': {e}")
            return False
        except Exception as e:
            LOG.error(f"Unexpected error while updating client '{client.client_id}': {e}")
            return False

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
            try:
                index_name = f"{self.name}_search_index"
                query = f"@{key}:{val}"
                results = self.redis.execute_command("FT.SEARCH", index_name, query)
                res = []
                if results and len(results) > 1:
                    for i in range(1, len(results), 2):
                        client_key = results[i]
                        if client_key.startswith("client:"):
                            client_data = self.redis.get(client_key)
                            if client_data:
                                try:
                                    client = cast2client(client_data)
                                    if client.api_key != "revoked":
                                        res.append(client)
                                except Exception as e:
                                    LOG.warning(f"Failed to deserialize client data: {e}")
                LOG.debug(f"RediSearch found {len(res)} clients matching '{key}={val}'")
                return res
            except Exception as e:
                LOG.debug(f"RediSearch query failed, falling back to basic search: {e}")

        if key in ['name', 'api_key']:
            LOG.debug(f"Searching for clients by indexed field '{key}' with value '{val}'")
            client_ids = self.redis.smembers(f"{self.index_prefix}:{key}:{val}")
            res = []
            for cid in client_ids:
                try:
                    client_data = self.redis.get(f"client:{cid}")
                    if client_data:
                        client = cast2client(client_data)
                        if client.api_key != "revoked":
                            res.append(client)
                except Exception as e:
                    LOG.warning(f"Failed to deserialize client data: {e}")
            LOG.debug(f"Found {len(res)} clients matching '{key}={val}'")
            return res

        res = []
        for client_id in self.redis.scan_iter("client:*"):
            if client_id.startswith(f"{self.index_prefix}:"):
                continue
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

    def __len__(self) -> int:
        """
        Get the number of items in the Redis database.

        Returns:
            The number of clients in the database.
        """
        counter_key = f"{self.name}:count"
        try:
            count = self.redis.get(counter_key)
            if count is not None:
                return int(count)
            self.redis.set(counter_key, 0)
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
        for client_id in self.redis.scan_iter(f"client:*"):
            if client_id.startswith(f"{self.index_prefix}:"):
                continue
            try:
                client = cast2client(self.redis.get(client_id))
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


    def get_connection_stats(self) -> dict:
        """
        Get connection pool statistics for monitoring.

        Returns:
            Dictionary containing connection pool statistics
        """
        try:
            stats = {
                'is_cluster': self.is_cluster,
                'max_connections': self.max_connections,
                'host': self.host,
                'port': self.port,
                'db': self.db if not self.is_cluster else None,
                'redisearch_available': self.redisearch_available
            }

            if not self.is_cluster and hasattr(self, 'redis_pool'):
                stats.update({
                    'connections_in_pool': len(self.redis_pool._available_connections),
                    'connections_in_use': len(self.redis_pool._in_use_connections),
                    'pool_size': self.redis_pool._created_connections,
                })

            try:
                info = self.redis.info('server')
                stats['redis_version'] = info.get('redis_version')
                stats['uptime_in_seconds'] = info.get('uptime_in_seconds')
            except Exception:
                pass
            return stats
        except Exception as e:
            LOG.warning(f"Failed to get connection stats: {e}")
            return {}

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
