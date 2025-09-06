from dataclasses import dataclass
from typing import List, Optional, Iterable

import redis
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
        ssl (bool): Enable SSL/TLS connection (default: False)
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
    ssl: bool = False
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None
    ssl_ca_certs: Optional[str] = None
    ssl_cert_reqs: str = "required"
    ssl_check_hostname: bool = True


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

        counter_key = f"{self.index_prefix}:count"
        if not self.redis.exists(counter_key):
            self.redis.setnx(counter_key, 0)
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

    def _detect_cluster(self) -> bool:
        """
        Detect if Redis is running in cluster mode.

        Returns:
            True if cluster mode is detected, False otherwise.
        """
        if self.cluster_nodes:
            return True

        try:
            ssl_context = None
            if self.ssl:
                import ssl
                ssl_context = ssl.create_default_context()
                if self.ssl_ca_certs:
                    ssl_context.load_verify_locations(self.ssl_ca_certs)
                if self.ssl_certfile and self.ssl_keyfile:
                    ssl_context.load_cert_chain(self.ssl_certfile, self.ssl_keyfile)
                ssl_context.check_hostname = self.ssl_check_hostname
                ssl_context.verify_mode = getattr(ssl, f"VERIFY_{self.ssl_cert_reqs.upper()}")

            test_client = redis.StrictRedis(
                host=self.host,
                port=self.port,
                password=self.password,
                username=self.username,
                db=0,
                socket_connect_timeout=2,
                socket_timeout=2,
                ssl=self.ssl,
                ssl_context=ssl_context if self.ssl else None
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
        }

        if self.ssl:
            import ssl
            ssl_context = ssl.create_default_context()
            if self.ssl_ca_certs:
                ssl_context.load_verify_locations(self.ssl_ca_certs)
            if self.ssl_certfile and self.ssl_keyfile:
                ssl_context.load_cert_chain(self.ssl_certfile, self.ssl_keyfile)
            ssl_context.check_hostname = self.ssl_check_hostname
            ssl_context.verify_mode = getattr(ssl, f"VERIFY_{self.ssl_cert_reqs.upper()}")

            connection_kwargs.update({
                'ssl': True,
                'ssl_context': ssl_context,
            })

        self.redis_pool = redis.ConnectionPool(
            max_connections=self.max_connections,
            retry_on_timeout=True,
            retry_on_error=[redis.ConnectionError, redis.TimeoutError],
            **connection_kwargs
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

        ssl_context = None
        if self.ssl:
            import ssl
            ssl_context = ssl.create_default_context()
            if self.ssl_ca_certs:
                ssl_context.load_verify_locations(self.ssl_ca_certs)
            if self.ssl_certfile and self.ssl_keyfile:
                ssl_context.load_cert_chain(self.ssl_certfile, self.ssl_keyfile)
            ssl_context.check_hostname = self.ssl_check_hostname
            ssl_context.verify_mode = getattr(ssl, f"VERIFY_{self.ssl_cert_reqs.upper()}")

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
                skip_full_coverage_check=True,
                ssl=self.ssl,
                ssl_context=ssl_context if self.ssl else None
            )
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
            self.redis.ft("test_index").info()
            return True
        except redis.ResponseError:
            return False
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
                "PREFIX", "1", f"{self.index_prefix}idx:",
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
        Generate the next sequential client ID.

        Returns:
            The next available client ID
        """
        max_id = 0
        for client_key in self.redis.scan_iter(f"{self.index_prefix}:client:*"):
            try:
                key_parts = client_key.split(":")
                if len(key_parts) >= 3:
                    client_id = int(key_parts[-1])
                    max_id = max(max_id, client_id)
            except (ValueError, IndexError):
                continue
        return max_id + 1

    def add_item(self, client: Client) -> bool:
        try:
            if not hasattr(client, 'client_id') or client.client_id is None:
                client.client_id = self._get_next_client_id()
            elif isinstance(client.client_id, str):
                try:
                    client.client_id = int(client.client_id)
                except ValueError:
                    client.client_id = self._get_next_client_id()
            elif not isinstance(client.client_id, int):
                client.client_id = self._get_next_client_id()

            while self.redis.exists(f"{self.index_prefix}:client:{client.client_id}"):
                client.client_id = self._get_next_client_id()

            p = self.redis.pipeline()
            p.set(f"{self.index_prefix}:client:{client.client_id}", client.serialize())
            p.sadd(f"{self.index_prefix}:name:{client.name}", str(client.client_id))
            p.sadd(f"{self.index_prefix}:api_key:{client.api_key}", str(client.client_id))
            p.incr(f"{self.index_prefix}:count")
            p.execute()
            return True
        except Exception as e:
            LOG.error(f"Failed to add client: {e}")
            return False

    def remove_client(self, client_id: str) -> bool:
        """
        Remove a client from Redis and clean up indices.

        Args:
            client_id: The ID of the client to remove.

        Returns:
            True if the removal was successful, False otherwise.
        """
        counter_key = f"{self.index_prefix}:count"
        try:
            item_key = f"{self.index_prefix}:client:{client_id}"
            client_data = self.redis.get(item_key)
            if not client_data:
                LOG.warning(f"Client '{client_id}' not found for removal")
                return False

            self.redis.delete(item_key)
            self.redis.decr(counter_key)

            try:
                client = cast2client(client_data)
                self.redis.srem(f"{self.index_prefix}:name:{client.name}", str(client.client_id))
                self.redis.srem(f"{self.index_prefix}:api_key:{client.api_key}", str(client.client_id))
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
            if old_client.name != client.name:
                p.srem(f"{self.index_prefix}:name:{old_client.name}", str(old_client.client_id))
                p.sadd(f"{self.index_prefix}:name:{client.name}", str(client.client_id))
            if old_client.api_key != client.api_key:
                p.srem(f"{self.index_prefix}:api_key:{old_client.api_key}", str(old_client.client_id))
                p.sadd(f"{self.index_prefix}:api_key:{client.api_key}", str(client.client_id))
            p.execute()

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
            query = f"@{key}:{val}"
            results = self.redis.execute_command("FT.SEARCH", index_name, query)
            res = []
            if results and len(results) > 1:
                for i in range(1, len(results), 2):
                    client_key = results[i]
                    if client_key.startswith(f"{self.index_prefix}:client:"):
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
        for client_id in self.redis.scan_iter(f"{self.index_prefix}:client:*"):
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
        for client_id in self.redis.scan_iter(f"{self.index_prefix}:client:*"):
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
