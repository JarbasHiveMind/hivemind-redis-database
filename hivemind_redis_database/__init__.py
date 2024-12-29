from dataclasses import dataclass
from typing import List, Union, Optional, Iterable

import redis
from ovos_utils.log import LOG

from hivemind_plugin_manager.database import Client, AbstractRemoteDB, cast2client


@dataclass
class RedisDB(AbstractRemoteDB):
    """Database implementation using Redis with RediSearch support."""
    host: str = "127.0.0.1"
    port: int = 6379
    name: str = "clients"
    subfolder: str = "hivemind-core"
    password: Optional[str] = None
    database_id: Optional[int] = None

    def __post_init__(self):
        """
        Initialize the RedisDB connection.
        """
        self.redis = redis.StrictRedis(host=self.host, port=self.port, db=self.database_id,
                                       password=self.password if self.password else None,
                                       decode_responses=True)
        # TODO - support for a proper search index

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
        try:
            # Store data in Redis
            self.redis.set(item_key, serialized_data)

            # Maintain indices for common search fields
            self.redis.sadd(f"client:index:name:{client.name}", client.client_id)
            self.redis.sadd(f"client:index:api_key:{client.api_key}", client.client_id)
            return True
        except Exception as e:
            LOG.error(f"Failed to add client to Redis/RediSearch: {e}")
            return False

    def search_by_value(self, key: str, val: Union[str, bool, int, float]) -> List[Client]:
        """
        Search for clients by a specific key-value pair in Redis.

        Args:
            key: The key to search by.
            val: The value to search for.

        Returns:
            A list of clients that match the search criteria.
        """
        # Use index if available
        if key in ['name', 'api_key']:
            client_ids = self.redis.smembers(f"client:index:{key}:{val}")
            res = [cast2client(self.redis.get(f"client:{cid}"))
                   for cid in client_ids]
            res = [c for c in res if c.api_key != "revoked"]
            return res

        res = []
        for client_id in self.redis.scan_iter(f"client:*"):
            if client_id.startswith("client:index:"):
                continue
            client_data = self.redis.get(client_id)
            client = cast2client(client_data)
            if hasattr(client, key) and getattr(client, key) == val:
                res.append(client)
        return res

    def __len__(self) -> int:
        """
        Get the number of items in the Redis database.

        Returns:
            The number of clients in the database.
        """
        return int(len(self.redis.keys("client:*")) / 3)  # because of index entries for name/key fastsearch

    def __iter__(self) -> Iterable['Client']:
        """
        Iterate over all clients in Redis.

        Returns:
            An iterator over the clients in the database.
        """
        for client_id in self.redis.scan_iter(f"client:*"):
            if client_id.startswith("client:index:"):
                continue
            try:
                yield cast2client(self.redis.get(client_id))
            except Exception as e:
                LOG.error(f"Failed to get client '{client_id}' : {e}")
